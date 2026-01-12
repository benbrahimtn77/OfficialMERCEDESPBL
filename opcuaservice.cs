using WorkplaceClient.Language;
using MudBlazor;
using WorkplaceClient.Shared;
using WorkplaceClient.Services.Interfaces;
using System;
using System.Threading.Tasks;
using MESShared.Shared.Models.Resources;
using APIGateway;
using MESShared.Shared.Models.Events;
using System.Collections.Concurrent;
using OpcUaClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using MESShared.Shared.Models.Orders;
using CommunicationProtocol.Core.Events;
using CommunicationProtocol.Core.Common;
using CommunicationProtocol.Core.Interfaces;

namespace WorkplaceClient.Services
{
    public class OpcUaService : IOpcUaService
    {
        private readonly ITranslator _translator;
        private readonly IWorkplaceService _workplaceService;
        private readonly IEventPublisher _eventPublisher;
        private readonly IOpcUaClient _opcUaClient;
        private readonly IClient _client;
        private readonly IAuthenticationService _authenticationService;
        
        private bool _isInitialized = false;
        private bool _isConnected = false;
        private bool _subscriptionsInitialized = false;
        private readonly object _initializationLock = new object();
        private Bundle _currentBundle;
        private int _currentStep;
        private OpcUaProductionOutputData _lastProductionOutputData;
        private List<OpcUaBoxIdData> _lastBoxIdsData = new();

        public event Action<string, Severity> OnUserInformation;
        public event Action<int> CheckingResponseChanged;
        public event Action<bool> OnConnectionStatusChanged;
        public event Action<int> OnStatusChanged;
        public event Action<Bundle> OnBundleStarted;
        public event Action<Bundle> OnBundleFinished;

        public OpcUaService(
            ITranslator translator, 
            IWorkplaceService workplaceService, 
            IEventPublisher eventPublisher, 
            IOpcUaClient opcUaClient,
            IClient client,
            IAuthenticationService authenticationService)
        {
            _translator = translator;
            _workplaceService = workplaceService;
            _eventPublisher = eventPublisher;
            _opcUaClient = opcUaClient;
            _client = client;
            _authenticationService = authenticationService;
            
            _ = Task.Run(() => InitializeSubscriptions());
            _ = Task.Run(() => SubscribeToEventPublisherEvents());
            _workplaceService.WorkplaceUpdated += OnWorkplaceUpdated;
        }
        private void OnWorkplaceUpdated()
        {
            try
            {
                _ = Task.Run(async () => 
                {
                  
                    await Task.Delay(500);
                    await InitializeIfConfiguredAsync();
                });
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke($"Error: {ex.Message}", Severity.Error);
            }
        }

        private void SubscribeToEventPublisherEvents()
        {
            try
            {
                _eventPublisher.Subscribe<OpcUaStopRequestEvent>(async evt => await HandleOpcUaStopRequest(evt));
                _eventPublisher.Subscribe<OpcUaReadProductionOutputResponseEvent>(async evt => await HandleProductionOutputResponse(evt));
                _eventPublisher.Subscribe<OpcUaReadBoxIdsResponseEvent>(async evt => await HandleBoxIdsResponse(evt));
                _eventPublisher.Subscribe<OpcUaResetStatusResponseEvent>(async evt => await HandleResetStatusResponse(evt));
                _eventPublisher.Subscribe<OpcUaClearOutputNodesResponseEvent>(async evt => await HandleClearOutputNodesResponse(evt));
                
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Subscribed to EventPublisher control events");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error subscribing to EventPublisher: {ex}");
            }
        }

        private async Task HandleOpcUaStopRequest(OpcUaStopRequestEvent evt)
        {
            try
            {
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Received OpcUaStopRequestEvent");
                await DisconnectAsync();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error in HandleOpcUaStopRequest: {ex}");
            }
        }

        private async Task HandleProductionOutputResponse(OpcUaReadProductionOutputResponseEvent evt)
        {
            try
            {
                if (!evt.Success)
                {
                    OnUserInformation?.Invoke(_translator[$"Error reading OPC UA output data: {evt.Error}"], Severity.Error);
                    return;
                }

                _lastProductionOutputData = evt.OutputData;
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Received production output data via event");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error handling production output response: {ex}");
            }
        }

        private async Task HandleBoxIdsResponse(OpcUaReadBoxIdsResponseEvent evt)
        {
            try
            {
                if (!evt.Success)
                {
                    OnUserInformation?.Invoke(_translator[$"Error reading box IDs: {evt.Error}"], Severity.Error);
                    return;
                }

                _lastBoxIdsData = evt.BoxIds;
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Received box IDs data via event");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error handling box IDs response: {ex}");
            }
        }

        private async Task HandleResetStatusResponse(OpcUaResetStatusResponseEvent evt)
        {
            try
            {
                if (!evt.Success)
                {
                    OnUserInformation?.Invoke(_translator[$"Warning: Could not reset status to 0: {evt.Error}"], Severity.Warning);
                    return;
                }

                OnUserInformation?.Invoke(_translator["Machine ready for next product (status = 0)"], Severity.Success);
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Status reset to 0 via event");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error handling reset status response: {ex}");
            }
        }

        private async Task HandleClearOutputNodesResponse(OpcUaClearOutputNodesResponseEvent evt)
        {
            try
            {
                if (!evt.Success)
                {
                    OnUserInformation?.Invoke(_translator[$"Warning: Could not clear all output nodes: {evt.Error}"], Severity.Warning);
                    return;
                }

                OnUserInformation?.Invoke(_translator["Static.status All output nodes cleared"], Severity.Success);
                System.Diagnostics.Debug.WriteLine("[OpcUaService] Output nodes cleared via event");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error handling clear output nodes response: {ex}");
            }
        }

        private void InitializeSubscriptions()
        {
            lock (_initializationLock)
            {
                if (_subscriptionsInitialized)
                    return;

                try
                {
                    SubscribeToOpcUaEvents();
                    _subscriptionsInitialized = true;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error initializing subscriptions: {ex}");
                }
            }
        }

        private void SubscribeToOpcUaEvents()
        {
            if (_opcUaClient == null)
            {
                System.Diagnostics.Debug.WriteLine("[OpcUaService] IOpcUaClient is null");
                return;
            }

            try
            {
              
                _opcUaClient.OnInfo += (msg) =>
                {
                    OnUserInformation?.Invoke(msg, Severity.Info);
                    _eventPublisher?.Publish(new OpcUaConnectedEvent { Endpoint = msg, Timestamp = DateTime.UtcNow });
                };

                _opcUaClient.OnError += (msg) =>
                {
                    OnUserInformation?.Invoke(msg, Severity.Error);
                    _eventPublisher?.Publish(new OpcUaConnectionFailedEvent { ErrorMessage = msg });
                };
                
                _opcUaClient.OnNodeChanged += (nodeName, value) =>
                {
                    System.Diagnostics.Debug.WriteLine($"[OpcUaService] Node changed: {nodeName} = {value}");
                    if (nodeName.Contains("Employee.Checking") || nodeName.Contains("Employee") && nodeName.Contains("Checking"))
                    {
                        System.Diagnostics.Debug.WriteLine($"[OpcUaService] Employee.Checking changed to {value} - triggered by OpcUaClient subscription");
                    }
                };
                
                OnUserInformation?.Invoke(_translator["OPC UA handlers registered"], Severity.Info);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[OpcUaService] Error subscribing to IOpcUaClient events: {ex}");
            }
        }

        public async Task InitializeIfConfiguredAsync()
        {
            if (!IsOpcUaConfigured())
            {
                return;
            }

            await AutoInitializeAsync();
        }

        public async Task AutoInitializeAsync()
        {
            if (_isInitialized && _isConnected)
            {
                OnUserInformation?.Invoke(_translator["OPC UA already connected"], Severity.Info);
                return;
            }

            try
            {

                var opcUaResource = _workplaceService?.GetLinkedResourceByType(ResourceTypes.OpcUaSettings);

                if (opcUaResource == null)
                {
                    return;
                }

                var opcUaSettings = TryParseOpcUaSettings(opcUaResource.Parameters);
                if (opcUaSettings == null || string.IsNullOrEmpty(opcUaSettings.Endpoint))
                {
                    OnUserInformation?.Invoke(_translator["Invalid OPC UA configuration"], Severity.Warning);
                    return;
                }

                var workplace = _workplaceService?.workplace?.Name;
                var equipment = _workplaceService?.workplace?.CurrentEquipment?.Name;

                if (string.IsNullOrEmpty(workplace) || string.IsNullOrEmpty(equipment))
                {
                    return;
                }

                _isInitialized = true;
                
                OnUserInformation?.Invoke(_translator["Connecting to OPC UA server..."], Severity.Info);
                
              
                bool connectComplete = false;
                bool connectSuccess = false;
                
                Action<OpcUaConnectResponseEvent> connectHandler = null;
                connectHandler = (e) =>
                {
                    connectSuccess = e.Success;
                    connectComplete = true;
                    _eventPublisher?.Unsubscribe(connectHandler);
                };
                _eventPublisher?.Subscribe(connectHandler);
                
                _eventPublisher?.Publish(new OpcUaConnectRequestEvent2 
                { 
                    Endpoint = opcUaSettings.Endpoint,
                    UseSecurity = false,
                    NamespaceIndex = opcUaSettings.NamespaceIndex
                });

                int elapsed = 0;
                while (!connectComplete && elapsed < 10000)
                {
                    await Task.Delay(100);
                    elapsed += 100;
                }

                if (!connectSuccess)
                {
                    OnUserInformation?.Invoke(_translator["Failed to connect"], Severity.Error);
                    _isInitialized = false;
                    return;
                }

                _isConnected = true;
                OnConnectionStatusChanged?.Invoke(true);
                _eventPublisher?.Publish(new OpcUaConnectedEvent { Endpoint = opcUaSettings.Endpoint });

                OnUserInformation?.Invoke(_translator["Validating workplace and equipment..."], Severity.Info);
                
                bool validateComplete = false;
                bool validateSuccess = false;
                string validateError = "";
                
                Action<OpcUaValidateWorkplaceEquipmentResponseEvent> validateHandler = null;
                validateHandler = (e) =>
                {
                    validateSuccess = e.IsValid;
                    validateError = e.ErrorMessage ?? "";
                    validateComplete = true;
                    _eventPublisher?.Unsubscribe(validateHandler);
                };
                _eventPublisher?.Subscribe(validateHandler);
                
                _eventPublisher?.Publish(new OpcUaValidateWorkplaceEquipmentRequestEvent 
                { 
                    Workplace = workplace,
                    Equipment = equipment
                });

                elapsed = 0;
                while (!validateComplete && elapsed < 10000)
                {
                    await Task.Delay(100);
                    elapsed += 100;
                }

                if (!validateSuccess)
                {
                    OnUserInformation?.Invoke(_translator[$"Workplace/Equipment validation failed: {validateError}"], Severity.Error);
                    _eventPublisher?.Publish(new OpcUaConnectionFailedEvent { 
                        ErrorMessage = validateError ?? "Workplace/Equipment mismatch" 
                    });
                    _isInitialized = false;
                    _isConnected = false;
                    OnConnectionStatusChanged?.Invoke(false);

                    _eventPublisher?.Publish(new OpcUaDisconnectRequestEvent());
                    
                    return;
                }

                OnUserInformation?.Invoke(_translator[$"Static.status Workplace '{workplace}' and Equipment '{equipment}' validated"], Severity.Success);

                // START WORKPLACE/EQUIPMENT MONITORING 
                try
                {
                    bool monitoringComplete = false;
                    Action<OpcUaWorkplaceEquipmentMonitoringErrorEvent> monitoringErrorHandler = null;
                    monitoringErrorHandler = (e) =>
                    {
                        OnUserInformation?.Invoke(_translator[$"Workplace/Equipment mismatch: {e.ErrorMessage}"], Severity.Error);
                        _eventPublisher?.Publish(new OpcUaConnectionFailedEvent { ErrorMessage = e.ErrorMessage });
                        _eventPublisher?.Unsubscribe(monitoringErrorHandler);
                    };
                    _eventPublisher?.Subscribe(monitoringErrorHandler);
                    
                    _eventPublisher?.Publish(new OpcUaStartWorkplaceEquipmentMonitoringRequestEvent 
                    { 
                        Workplace = workplace,
                        Equipment = equipment
                    });
                    
                    OnUserInformation?.Invoke(_translator["Workplace/Equipment monitoring started"], Severity.Info);
                }
                catch (Exception wpEx)
                {
                    OnUserInformation?.Invoke(_translator[$"Failed to start workplace/equipment monitoring: {wpEx.Message}"], Severity.Warning);
                }

 
                bool isLoginOnMachineEnabled = IsLoginOnMachineEnabled();

                // START EMPLOYEE MONITORING IF LoginOnMachine ENABLED
                if (isLoginOnMachineEnabled)
                {
                    try
                    {
                        OnUserInformation?.Invoke(_translator["Starting employee monitoring..."], Severity.Info);

                        await _opcUaClient.StartEmployeeMonitoringAsync(ValidateEmployeeAsync);
                        
                        OnUserInformation?.Invoke(_translator["Static.status Employee monitoring started - listening to Employee.Checking node"], Severity.Success);
                    }
                    catch (Exception empEx)
                    {
                        OnUserInformation?.Invoke(_translator[$"Failed to start employee monitoring: {empEx.Message}"], Severity.Warning);
                    }
                }
                else
                {
                    OnUserInformation?.Invoke(_translator["LoginOnMachine resource not linked - employee monitoring disabled"], Severity.Info);
                }

                // Start heartbeat monitoring
                int heartbeatInterval = opcUaSettings.MesCheckingInterval ?? 5;
                
                Action<OpcUaHeartbeatErrorEvent> heartbeatErrorHandler = null;
                heartbeatErrorHandler = (e) =>
                {
                    OnUserInformation?.Invoke(e.ErrorMessage, Severity.Error);
                    _eventPublisher?.Publish(new OpcUaConnectionFailedEvent { ErrorMessage = e.ErrorMessage });
                    _eventPublisher?.Unsubscribe(heartbeatErrorHandler);
                };
                _eventPublisher?.Subscribe(heartbeatErrorHandler);
                
                _eventPublisher?.Publish(new OpcUaStartMesHeartbeatRequestEvent 
                { 
                    HeartbeatInterval = heartbeatInterval,
                    Workplace = workplace,
                    Equipment = equipment
                });

                // Start monitoring CheckingResponse for production start
                await _opcUaClient.StartCheckingResponseMonitoringAsync(OnCheckingResponseMonitored);
                OnUserInformation?.Invoke(_translator["CheckingResponse monitoring started"], Severity.Info);

                await _opcUaClient.StartStatusMonitoringAsync(OnStatusMonitored);
                OnUserInformation?.Invoke(_translator["Status monitoring started"], Severity.Info);

                OnUserInformation?.Invoke(_translator["Static.status OPC UA monitoring fully initialized"], Severity.Success);
            }
            catch (Exception ex)
            {
                _isInitialized = false;
                OnUserInformation?.Invoke(_translator[$"OPC UA error: {ex.Message}"], Severity.Error);
            }
        }
        public async Task<(bool isValid, string fullName)> ValidateEmployeeAsync(string personalId)
        {
            try
            {
                if (string.IsNullOrEmpty(personalId))
                {
                    OnUserInformation?.Invoke(_translator["Employee logout detected"], Severity.Info);
                    return (true, string.Empty);
                }

                OnUserInformation?.Invoke(_translator[$"Validating employee from OPC UA: {personalId}"], Severity.Info);

                var user = await _client.UserGetAsync(personalId);

                if (user == null)
                {
                    OnUserInformation?.Invoke(_translator[$"Employee not found in MES: {personalId}"], Severity.Error);
                    return (false, string.Empty);
                }

                if (user.Locked == true)
                {
                    OnUserInformation?.Invoke(_translator[$"Employee is locked: {personalId} - Reason: {user.LockReason}"], Severity.Error);
                    return (false, string.Empty);
                }

                string fullName = $"{user.FirstName} {user.SecondName}".Trim();
                OnUserInformation?.Invoke(_translator[$"Employee validated: {fullName}"], Severity.Success);

                return (true, fullName);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error validating employee: {ex.Message}"], Severity.Error);
                return (false, string.Empty);
            }
        }

        private void OnCheckingResponseMonitored(int value)
        {
            CheckingResponseChanged?.Invoke(value);
            
   
            if (value == 0)
            {
                _ = Task.Run(() => HandleCheckingResponse2StartProductionAsync());
            }
        }

        private async Task HandleCheckingResponse2StartProductionAsync()
        {
            try
            {
                OnUserInformation?.Invoke(_translator["CheckingResponse=2: Starting production..."], Severity.Info);
                
                var workplace = _workplaceService?.workplace?.Name;
                var equipment = _workplaceService?.workplace?.CurrentEquipment?.Name;
                var userId = _authenticationService?.User?.PersonalID;

                // VALIDATE WORKPLACE/EQUIPMENT
                OnUserInformation?.Invoke(_translator["Validating workplace and equipment..."], Severity.Info);
                bool validateComplete = false;
                bool validateSuccess = false;
                string validateError = "";
                
                Action<OpcUaValidateWorkplaceEquipmentResponseEvent> validateHandler = null;
                validateHandler = (e) =>
                {
                    validateSuccess = e.IsValid;
                    validateError = e.ErrorMessage ?? "";
                    validateComplete = true;
                    _eventPublisher?.Unsubscribe(validateHandler);
                };
                _eventPublisher?.Subscribe(validateHandler);
                
                _eventPublisher?.Publish(new OpcUaValidateWorkplaceEquipmentRequestEvent 
                { 
                    Workplace = workplace,
                    Equipment = equipment
                });

                int elapsed = 0;
                while (!validateComplete && elapsed < 10000)
                {
                    await Task.Delay(100);
                    elapsed += 100;
                }

                if (!validateSuccess)
                {
                    OnUserInformation?.Invoke(_translator[$" Workplace/Equipment validation failed: {validateError}"], Severity.Error);
                    _eventPublisher?.Publish(new OpcUaCheckingResponseProcessedEvent { ResultCode = 2, Message = validateError });
                    return;
                }

                OnUserInformation?.Invoke(_translator["Static.status Workplace/Equipment validated"], Severity.Success);
                bool isLoginOnMachineResourceExists = IsLoginOnMachineEnabled();

                if (isLoginOnMachineResourceExists)
                {
                    OnUserInformation?.Invoke(_translator["Validating employee for production..."], Severity.Info);
                    
                    var employeeValidation = await _opcUaClient.ValidateEmployeeForProductionAsync(
                        workplace,
                        equipment,
                        async (wp, eq) => 
                        {
                            try
                            {
                                var mesLoggedUser = await _client.UserLoggedUserWorkplaceEquipmentAsync(wp, eq);
                                return mesLoggedUser?.PersonalID;
                            }
                            catch
                            {
                                return null;
                            }
                        }
                    );

                    if (!employeeValidation.isValid)
                    {
                        OnUserInformation?.Invoke(_translator[$" Employee validation failed: {employeeValidation.errorMessage}"], Severity.Error);
                        _eventPublisher?.Publish(new OpcUaCheckingResponseProcessedEvent { ResultCode = 2, Message = employeeValidation.errorMessage });
                        return;
                    }

                    OnUserInformation?.Invoke(_translator[$"Static.status Employee validated: {employeeValidation.employeeId}"], Severity.Success);
                }
                else
                {
                    OnUserInformation?.Invoke(_translator["LoginOnMachine resource not linked - skipping employee verification, production can start normally"], Severity.Info);
                }

                
                try
                {
                    OnUserInformation?.Invoke(_translator["Clearing box ID scan table..."], Severity.Info);
                    await _client.WarehouseClearBoxIdScanAsync(workplace, equipment);
                    OnUserInformation?.Invoke(_translator["Static.status Box ID scans cleared"], Severity.Success);
                }
                catch (Exception ex)
                {
                    OnUserInformation?.Invoke($"Warning: Could not clear box ID scans: {ex.Message}", Severity.Warning);
                }

                string extractedExternalId = null;

                (bool success, string? errorMessage) result = await _opcUaClient.StartProductionByExternalIdAsync(
                    bundleLookupCallback: async (externalId) =>
                    {
                        try
                        {
                            OnUserInformation?.Invoke(_translator[$"External ID (raw): {externalId}"], Severity.Info);
         
                            var scanExternalIdResource = _workplaceService.GetLinkedResourceByType(ResourceTypes.ScanExternalId);
                            string scanExternalIdPattern = scanExternalIdResource?.Parameters ?? "";

                            OnUserInformation?.Invoke(_translator[$"Pattern: {scanExternalIdPattern}"], Severity.Info);

                            var validationResult = await ValidateAndExtractExternalId(externalId, scanExternalIdPattern);
                            
                            if (validationResult.status == ScanService.ExternalIdStatus.Invalid)
                            {
                                OnUserInformation?.Invoke(_translator[" Invalid External ID format"], Severity.Error);
                                return null;
                            }
                            
                            extractedExternalId = validationResult.extractedId?.Trim() ?? externalId;
                            OnUserInformation?.Invoke(_translator[$" Extracted External ID: {extractedExternalId}"], Severity.Success);
                            
                            return await _client?.OrdersGetExternalBundleAsync(extractedExternalId);
                        }
                        catch (ApiException ex) when (ex.StatusCode == 204)
                        {
                            return null;
                        }
                    },
                    masterDataLookupCallback: async (orderNumber) =>
                    {
                        try
                        {
                            return await _client?.OrdersGetOrderMasterDataAsync(orderNumber);
                        }
                        catch (ApiException ex) when (ex.StatusCode == 204)
                        {
                            return null; 
                        }
                    },
                    startProductionCallback: async (wp, externalId, uid, eq) =>
                    {
                        try
                        {
                          
                            return await _client?.DispatcherStartProductionAsync(workplace, extractedExternalId ?? externalId, userId, equipment, true);
                        }
                        catch (ApiException ex) when (ex.StatusCode == 204)
                        {
                          
                            return new { Ok = true, Message = "Production started" };
                        }
                    },
                    allBundlesLookupCallback: async (orderNumber) =>
                    {
                        try
                        {
                            return await _client?.OrdersGetBundlesAsync(orderNumber);
                        }
                        catch (ApiException ex) when (ex.StatusCode == 204)
                        {
                            return null; 
                        }
                    },
                    onStatusCallback: (msg) =>
                    {
                        OnUserInformation?.Invoke(msg, Severity.Info);
                    }
                );

                if (result.success)
                {
                    OnUserInformation?.Invoke(_translator["Static.status Production started successfully"], Severity.Success);
                    _eventPublisher?.Publish(new OpcUaCheckingResponseProcessedEvent { ResultCode = 1, Message = "Production started" });
                    
                    try
                    {
                        var ordersQueue = await _client.DispatcherGetQueueAsync(workplace);
                        var startedBundle = ordersQueue?.Orders?.SelectMany(o => o.Bundles).FirstOrDefault(b => b.Status == "Started");
                        if (startedBundle != null)
                        {
                            _currentBundle = startedBundle;
                            _currentStep = startedBundle.CurrentStep ?? 0;
                            OnBundleStarted?.Invoke(startedBundle);
                        }
                    }
                    catch (Exception ex)
                    {
                        OnUserInformation?.Invoke(_translator[$"Error fetching started bundle: {ex.Message}"], Severity.Error);
                    }
                }
                else
                {
                    OnUserInformation?.Invoke(_translator[$" Production start failed: {result.errorMessage}"], Severity.Error);
                    _eventPublisher?.Publish(new OpcUaCheckingResponseProcessedEvent { ResultCode = 2, Message = result.errorMessage });
                }
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error: {ex.Message}"], Severity.Error);
            }
        }

        private void OnStatusMonitored(int status)
        {
            OnStatusChanged?.Invoke(status);
            
            if (status == 2)
            {
                _ = Task.Run(() => HandleStatus2FinishBundleAsync());
            }
        }

        private async Task HandleStatus2FinishBundleAsync()
        {
            try
            {
                var workplace = _workplaceService?.workplace?.Name;
                var equipment = _workplaceService?.workplace?.CurrentEquipment?.Name;
                var userId = _authenticationService?.User?.PersonalID;

                OnUserInformation?.Invoke(_translator["Status=2: Finishing bundle..."], Severity.Info);

                if (string.IsNullOrEmpty(workplace) || string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(equipment))
                {
                    OnUserInformation?.Invoke(_translator["Missing workplace or user information"], Severity.Error);
                    return;
                }

                var bundleToFinish = _currentBundle ?? (await _client.DispatcherGetQueueAsync(workplace))?.Orders?.SelectMany(o => o.Bundles).FirstOrDefault(b => b.Status == "Started");
                
                if (bundleToFinish == null)
                {
                    OnUserInformation?.Invoke(_translator["No started bundle found"], Severity.Warning);
                    return;
                }

                // Read OPC UA output data
                _lastProductionOutputData = null;
                _eventPublisher.Publish(new OpcUaReadProductionOutputRequestEvent());
                int elapsedMs = 0;
                while (_lastProductionOutputData == null && elapsedMs < 10000)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (_lastProductionOutputData == null)
                {
                    OnUserInformation?.Invoke(_translator["Error: Timeout reading OPC UA output data"], Severity.Error);
                    return;
                }

              
                bundleToFinish.ProducedQuantity = _lastProductionOutputData.GoodPart;
                bundleToFinish.WasteQuantity = _lastProductionOutputData.WastePart;

                string semiFinishedAction = _workplaceService?.GetSemiFinishedAction();

                // Call finish production
                var result = await _client.DispatcherFinishProductionAsync(
                    workplace, bundleToFinish.BundleID, bundleToFinish.CurrentStep ?? 0, userId,
                    bundleToFinish.ProducedQuantity, bundleToFinish.WasteQuantity,
                    bundleToFinish.ProducedSetUp, bundleToFinish.WasteSetUp,
                    semiFinishedAction, equipment
                );

                if (result?.Ok == true)
                {
                    OnUserInformation?.Invoke(_translator[$"Static.status Bundle {bundleToFinish.BundleID} finished"], Severity.Success);
                    
                    // Process box IDs from OPC UA
                    await ProcessBoxIdsFromOpcUaAsync(workplace, equipment);
                    
                    // Insert traceability data
                    var parameters = string.Join("; ", _lastProductionOutputData.Parameters.Select(p => $"{p.Name}={p.Value}"));
                    await InsertTraceabilityFromOpcUaAsync(bundleToFinish, parameters, _lastProductionOutputData.StartTime, _lastProductionOutputData.FinishTime);
                    
                    OnBundleFinished?.Invoke(bundleToFinish);
                    _currentBundle = null;
                    _currentStep = 0;

                    // Reset OPC UA output nodes
                    _eventPublisher.Publish(new OpcUaResetStatusRequestEvent());
                    await Task.Delay(500);
                    _eventPublisher.Publish(new OpcUaClearOutputNodesRequestEvent());
                }
                else
                {
                    OnUserInformation?.Invoke(_translator[result?.Message ?? "Error finishing bundle"], Severity.Error);
                }
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error finishing bundle: {ex.Message}"], Severity.Error);
            }
        }

        private async Task ProcessBoxIdsFromOpcUaAsync(string workplace, string equipment)
        {
            try
            {
                _lastBoxIdsData = new List<OpcUaBoxIdData>();
                _eventPublisher.Publish(new OpcUaReadBoxIdsRequestEvent());
                int timeoutMs = 10000;
                int elapsedMs = 0;
                while (_lastBoxIdsData.Count == 0 && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                int processedCount = 0;
                foreach (var boxIdData in _lastBoxIdsData)
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(boxIdData.UniqueId) && !string.IsNullOrEmpty(boxIdData.PartNumber))
                        {
                            await _client.WarehousePostBoxIdScanAsync(new BoxIdScan
                            {
                                WorkplaceName = workplace,
                                EquipmentName = equipment,
                                PartNumber = boxIdData.PartNumber,
                                BoxID = boxIdData.UniqueId,
                                ScannedAt = DateTime.Now
                            });
                            processedCount++;
                            OnUserInformation?.Invoke(_translator[$"Box ID {boxIdData.UniqueId} inserted"], Severity.Success);
                        }
                    }
                    catch (Exception ex)
                    {
                        OnUserInformation?.Invoke(_translator[$"Error processing box {boxIdData.UniqueId}: {ex.Message}"], Severity.Warning);
                    }
                }

                if (processedCount > 0)
                {
                    OnUserInformation?.Invoke(_translator[$"{processedCount} Box ID(s) inserted"], Severity.Success);
                }
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error processing box IDs: {ex.Message}"], Severity.Error);
            }
        }

        private async Task InsertTraceabilityFromOpcUaAsync(Bundle bundle, string parameters, string startTime, string finishTime)
        {
            try
            {
                if (bundle == null)
                {
                    OnUserInformation?.Invoke(_translator["Cannot insert traceability: No bundle data"], Severity.Warning);
                    return;
                }

                var traceability = new Traceability
                {
                    ItemID = bundle.BundleID,
                    OrderNumber = bundle.OrderNumber,
                    PartNumber = bundle.PartNumber ?? "",
                    Step = bundle.CurrentStep,
                    Workcenter = bundle.WorkCenter ?? "",
                    CommProtocol = "OPC UA Production Finish",
                    Insertion = DateTime.UtcNow,
                    Data = new List<TraceabilityData>()
                };

                if (!string.IsNullOrEmpty(parameters))
                {
                    traceability.Data.Add(new TraceabilityData { Key = "Parameter", Value = parameters });
                }
                if (!string.IsNullOrEmpty(startTime))
                {
                    traceability.Data.Add(new TraceabilityData { Key = "StartTime", Value = startTime });
                }
                if (!string.IsNullOrEmpty(finishTime))
                {
                    traceability.Data.Add(new TraceabilityData { Key = "FinishTime", Value = finishTime });
                }

                traceability.Data.Add(new TraceabilityData { Key = "ProducedQuantity", Value = bundle.ProducedQuantity?.ToString() ?? "0" });
                traceability.Data.Add(new TraceabilityData { Key = "WasteQuantity", Value = bundle.WasteQuantity?.ToString() ?? "0" });

                try
                {
                    await _client.TraceabilityPostAsync(traceability);
                    OnUserInformation?.Invoke(_translator["Traceability inserted"], Severity.Success);
                }
                catch (ApiException ex) when (ex.StatusCode == 204)
                {
                    OnUserInformation?.Invoke(_translator["Traceability inserted"], Severity.Success);
                }
                catch (Exception ex)
                {
                    OnUserInformation?.Invoke(_translator[$"Error inserting traceability: {ex.Message}"], Severity.Error);
                }
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error: {ex.Message}"], Severity.Error);
            }
        }

        public async Task<T> ReadNodeAsync<T>(string nodeIdentifier, TimeSpan? timeout = null)
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                if (!_isConnected)
                    throw new Exception("OPC UA not connected");

                string requestId = Guid.NewGuid().ToString();
                T result = default;
                bool readComplete = false;
                
                Action<OpcUaReadNodeResponseEvent> readHandler = null;
                readHandler = (e) =>
                {
                    if (e.RequestId == requestId)
                    {
                        if (e.Success && !string.IsNullOrEmpty(e.Value))
                        {
                            try
                            {
                                result = (T)Convert.ChangeType(e.Value, typeof(T));
                            }
                            catch
                            {
                                result = default;
                            }
                        }
                        readComplete = true;
                        _eventPublisher?.Unsubscribe(readHandler);
                    }
                };
                _eventPublisher?.Subscribe(readHandler);
                
                _eventPublisher?.Publish(new OpcUaReadNodeRequestEvent 
                { 
                    NodeIdentifier = nodeIdentifier,
                    RequestId = requestId
                });

               
                int timeoutMs = (int?)timeout?.TotalMilliseconds ?? 10000;
                int elapsedMs = 0;
                while (!readComplete && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (!readComplete)
                    throw new Exception($"Timeout reading node: {nodeIdentifier}");

                return result;
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error reading node: {ex.Message}"], Severity.Error);
                throw;
            }
        }

        public async Task WriteNodeAsync(string nodeIdentifier, object value)
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                if (!_isConnected)
                    throw new Exception("OPC UA not connected");

 
                bool writeComplete = false;
                bool writeSuccess = false;
                string writeError = "";
                
                Action<OpcUaWriteNodeResponseEvent> writeHandler = null;
                writeHandler = (e) =>
                {
                    if (e.NodeIdentifier == nodeIdentifier)
                    {
                        writeSuccess = e.Success;
                        writeError = e.Error;
                        writeComplete = true;
                        _eventPublisher?.Unsubscribe(writeHandler);
                    }
                };
                _eventPublisher?.Subscribe(writeHandler);
                
                _eventPublisher?.Publish(new OpcUaWriteNodeRequestEvent 
                { 
                    NodeIdentifier = nodeIdentifier,
                    Value = value
                });

                int timeoutMs = 10000;
                int elapsedMs = 0;
                while (!writeComplete && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (!writeComplete || !writeSuccess)
                {
                    throw new Exception($"Failed to write node: {nodeIdentifier}. Error: {writeError}");
                }

                OnUserInformation?.Invoke(_translator[$"Wrote {nodeIdentifier}"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error writing node: {ex.Message}"], Severity.Error);
                throw;
            }
        }
        public async Task StartCheckingResponseMonitoringAsync(Action<int> onCheckingResponseChanged = null)
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                await _opcUaClient.StartCheckingResponseMonitoringAsync(OnCheckingResponseMonitored);
                OnUserInformation?.Invoke(_translator["CheckingResponse monitoring started"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error starting monitoring: {ex.Message}"], Severity.Error);
            }
        }
        public async Task StopCheckingResponseMonitoringAsync()
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                OnUserInformation?.Invoke(_translator["CheckingResponse monitoring stopped"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error stopping monitoring: {ex.Message}"], Severity.Error);
            }
        }

        private MESShared.Shared.Models.Resources.OpcUaSettings TryParseOpcUaSettings(string parameters)
        {
            try
            {
                if (string.IsNullOrEmpty(parameters))
                    return null;

                return System.Text.Json.JsonSerializer.Deserialize<MESShared.Shared.Models.Resources.OpcUaSettings>(
                    parameters,
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Parse error: {ex.Message}"], Severity.Error);
                return null;
            }
        }

        public bool IsConnected => _isConnected;
        public bool IsInitialized => _isInitialized;


        private bool IsOpcUaConfigured()
        {
            try
            {
                var opcUaResource = _workplaceService?.GetLinkedResourceByType(ResourceTypes.OpcUaSettings);
                return opcUaResource != null;
            }
            catch
            {
                return false;
            }
        }

        private bool IsLoginOnMachineEnabled()
        {
            try
            {
                // if OpcUaSettings resource exists first
                var opcUaResource = _workplaceService?.GetLinkedResourceByType(ResourceTypes.OpcUaSettings);
                
                if (opcUaResource == null)
                {
                    return false;
                }

                // OPC UA Settings exists, check for LoginOnMachine resource
                var loginOnMachineResource = _workplaceService?.GetLinkedResourceByType(ResourceTypes.LoginOnMachine);
                return loginOnMachineResource != null;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> CheckConnectionHealthAsync()
        {
            try
            {
                return _isConnected;
            }
            catch
            {
                return false;
            }
        }
        public async Task WriteCheckingResponseAsync(int value)
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                bool writeComplete = false;
                bool writeSuccess = false;
                string writeError = "";
                
                Action<OpcUaSetCheckingResponseResponseEvent> handler = null;
                handler = (e) =>
                {
                    writeSuccess = e.Success;
                    writeError = e.Error ?? "";
                    writeComplete = true;
                    _eventPublisher?.Unsubscribe(handler);
                };
                _eventPublisher?.Subscribe(handler);
                
                _eventPublisher?.Publish(new OpcUaSetCheckingResponseRequestEvent { Value = value });
                int timeoutMs = 5000;
                int elapsedMs = 0;
                while (!writeComplete && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (!writeComplete || !writeSuccess)
                {
                    throw new Exception($"Failed to set CheckingResponse: {writeError}");
                }

                OnUserInformation?.Invoke(_translator[$"CheckingResponse set to {value}"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error setting CheckingResponse: {ex.Message}"], Severity.Error);
                throw;
            }
        }

        public async Task WriteOrderInputAsync(string bom, string parameter, string productId, string bundleId, int quantity, string partNumber, string externalId)
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                bool writeComplete = false;
                bool writeSuccess = false;
                string writeError = "";
                
                Action<OpcUaWriteOrderInputResponseEvent> handler = null;
                handler = (e) =>
                {
                    writeSuccess = e.Success;
                    writeError = e.Error ?? "";
                    writeComplete = true;
                    _eventPublisher?.Unsubscribe(handler);
                };
                _eventPublisher?.Subscribe(handler);
                    
                List<string> bomList = new();
                if (!string.IsNullOrEmpty(bom))
                {
                    bomList.Add(bom);
                }
                
                _eventPublisher?.Publish(new OpcUaWriteOrderInputRequestEvent 
                { 
                    Bom = bomList,
                    Parameter = parameter,
                    ProductId = productId,
                    BundleId = bundleId,
                    Quantity = quantity,
                    PartNumber = partNumber,
                    ExternalId = externalId
                });
                int timeoutMs = 5000;
                int elapsedMs = 0;
                while (!writeComplete && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (!writeComplete || !writeSuccess)
                {
                    throw new Exception($"Failed to write order input: {writeError}");
                }

                OnUserInformation?.Invoke(_translator["Order input written to machine"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error writing order input: {ex.Message}"], Severity.Error);
                throw;
            }
        }
        public async Task ClearOrderInputAsync()
        {
            if (!IsOpcUaConfigured())
                throw new Exception("OPC UA Settings resource not linked to this equipment");

            try
            {
                bool writeComplete = false;
                bool writeSuccess = false;
                string writeError = "";
                
                Action<OpcUaClearOrderInputResponseEvent> handler = null;
                handler = (e) =>
                {
                    writeSuccess = e.Success;
                    writeError = e.Error ?? "";
                    writeComplete = true;
                    _eventPublisher?.Unsubscribe(handler);
                };
                _eventPublisher?.Subscribe(handler);
                
                _eventPublisher?.Publish(new OpcUaClearOrderInputRequestEvent());
                int timeoutMs = 5000;
                int elapsedMs = 0;
                while (!writeComplete && elapsedMs < timeoutMs)
                {
                    await Task.Delay(100);
                    elapsedMs += 100;
                }

                if (!writeComplete || !writeSuccess)
                {
                    throw new Exception($"Failed to clear order input: {writeError}");
                }

                OnUserInformation?.Invoke(_translator["Order input cleared"], Severity.Info);
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error clearing order input: {ex.Message}"], Severity.Error);
                throw;
            }
        }
        public async Task DisconnectAsync()
        {
            try
            {
                _isConnected = false;
                
                _eventPublisher?.Publish(new OpcUaDisconnectRequestEvent());

                await Task.Delay(500);

                OnUserInformation?.Invoke(_translator["Disconnected from OPC UA"], Severity.Info);
                OnConnectionStatusChanged?.Invoke(false);
                _eventPublisher?.Publish(new OpcUaDisconnectedEvent());
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke(_translator[$"Error disconnecting: {ex.Message}"], Severity.Error);
                throw;
            }
        }
       public async Task<(bool success, string error)> StartOmegaFeedbackMonitoringAsync(
    Bundle bundle,
    Workplace workplace,
    object currentResources)
{
    try
    {
        if (!workplace.CurrentEquipment?.EquipmentValue?.CommunicationProtocol?.Contains("Omega") == true)
            return (true, null);

        var omegaResource = _workplaceService?.GetLinkedResourceByType(ResourceTypes.KomaxOmegaSettings);
        if (omegaResource == null)
        {
            OnUserInformation?.Invoke("[Omega] Komax Omega Settings resource not found - skipping", Severity.Info);
            return (true, null);
        }

        var omegaSettingsValue = omegaResource.GetType().GetProperty("KomaxOmegaSettingsValue")?.GetValue(omegaResource);
        if (omegaSettingsValue == null)
        {
            OnUserInformation?.Invoke("[Omega] Komax Omega Settings value is null", Severity.Warning);
            return (false, "Komax Omega Settings value is null");
        }
        string endpoint = omegaSettingsValue.GetType().GetProperty("Endpoint")?.GetValue(omegaSettingsValue) as string ?? "";
        string username = omegaSettingsValue.GetType().GetProperty("Username")?.GetValue(omegaSettingsValue) as string;
        string password = omegaSettingsValue.GetType().GetProperty("Password")?.GetValue(omegaSettingsValue) as string;
        int? namespaceIndex = omegaSettingsValue.GetType().GetProperty("NamespaceIndex")?.GetValue(omegaSettingsValue) as int?;

        if (string.IsNullOrEmpty(endpoint))
        {
            OnUserInformation?.Invoke("[Omega] Endpoint not configured", Severity.Warning);
            return (false, "Endpoint not configured");
        }

        var bundleId = bundle.BundleID ?? "";
        var orderNumber = bundle.OrderNumber ?? 0;
        var expectedPartNumber = bundle.PartNumber ?? "";

        OnUserInformation?.Invoke($"[Omega] Connecting to {endpoint}...", Severity.Info);

        bool connectComplete = false;
        bool connectSuccess = false;
        
        Action<OpcUaConnectResponseEvent> connectHandler = null;
        connectHandler = (e) =>
        {
            connectSuccess = e.Success;
            connectComplete = true;
            _eventPublisher?.Unsubscribe(connectHandler);
        };
        _eventPublisher?.Subscribe(connectHandler);
        
        _eventPublisher?.Publish(new OpcUaConnectRequestEvent2 
        { 
            Endpoint = endpoint,
            UseSecurity = false,
            NamespaceIndex = namespaceIndex ?? 3
        });

        int elapsed = 0;
        while (!connectComplete && elapsed < 10000)
        {
            await Task.Delay(100);
            elapsed += 100;
        }

        if (!connectSuccess)
        {
            OnUserInformation?.Invoke("[Omega] Connection failed", Severity.Error);
            return (false, "Connection failed");
        }

        OnUserInformation?.Invoke("[Omega] Connected successfully", Severity.Success);

        _eventPublisher.Subscribe<OpcUaOmegaProductionCompleteEvent>(async evt =>
        {
            await HandleOmegaProductionCompleteAsync(bundleId, orderNumber, expectedPartNumber, evt);
        });

        // Start monitoring
        await _opcUaClient.StartOmegaMonitoringAsync();

        OnUserInformation?.Invoke("[Omega] Monitoring started", Severity.Success);
        return (true, null);
    }
    catch (Exception ex)
    {
        OnUserInformation?.Invoke($"[Omega] Error: {ex.Message}", Severity.Error);
        return (false, ex.Message);
    }
}

private async Task HandleOmegaProductionCompleteAsync(
    string bundleId,
    long orderNumber,
    string expectedPartNumber,
    OpcUaOmegaProductionCompleteEvent evt)
{
    try
    {
        OnUserInformation?.Invoke($"[Omega] Production complete - UniqueId: {evt.UniqueId}", Severity.Info);

        var workplace = _workplaceService?.workplace?.Name;
        var equipment = _workplaceService?.workplace?.CurrentEquipment?.Name;
        var userId = _authenticationService?.User?.PersonalID;

        string expectedJobName = orderNumber.ToString();

        if (evt.JobName != expectedJobName || evt.ArticleName != expectedPartNumber)
        {
            string errorMsg = $"Inconsistent data! Expected Job='{expectedJobName}' Article='{expectedPartNumber}', " +
                            $"Got Job='{evt.JobName}' Article='{evt.ArticleName}'";
            OnUserInformation?.Invoke($"[Omega] {errorMsg}", Severity.Error);
            return;
        }

        OnUserInformation?.Invoke($"[Omega] Validation passed", Severity.Success);

        var bundle = await _client.DispatcherGetBundleAsync(bundleId);
        if (bundle == null)
        {
            OnUserInformation?.Invoke($"[Omega] Bundle not found", Severity.Error);
            return;
        }

        int goodPart = evt.ProductQuality == "Good" ? 1 : 0;
        int wastePart = evt.ProductQuality != "Good" ? 1 : 0; 

        OnUserInformation?.Invoke($"[Omega] Quality: {evt.ProductQuality} -> Good: {goodPart}, Waste: {wastePart}", Severity.Info);

        string semiFinishedAction = _workplaceService?.GetSemiFinishedAction();
        var finishResult = await _client.DispatcherFinishProductionAsync(
            workplace, bundleId, bundle.CurrentStep ?? 0, userId,
            goodPart, wastePart, 0, 0, semiFinishedAction, equipment);

        if (finishResult?.Ok == true)
        {
            OnUserInformation?.Invoke($"[Omega] Production finished - ExternalID '{evt.UniqueId}' assigned", Severity.Success);
            OnBundleFinished?.Invoke(bundle);
        }
        else
        {
            OnUserInformation?.Invoke($"[Omega] Failed: {finishResult?.Message}", Severity.Error);
            return;
        }

        var traceabilityData = new List<TraceabilityData>
        {
            new TraceabilityData { Key = "UniqueId", Value = evt.UniqueId },
            new TraceabilityData { Key = "ProductQuality", Value = evt.ProductQuality },
            new TraceabilityData { Key = "JobName", Value = evt.JobName },
            new TraceabilityData { Key = "ArticleName", Value = evt.ArticleName }
        };

        List<(string Key, string Value)> spliceData = new();
        bool traceabilityComplete = false;

        Action<OpcUaReadOmegaTraceabilityResponseEvent> traceHandler = null;
        traceHandler = (e) =>
        {
            if (e.Success)
                spliceData = e.TraceabilityData;
            traceabilityComplete = true;
            _eventPublisher?.Unsubscribe(traceHandler);
        };
        _eventPublisher?.Subscribe(traceHandler);

        _eventPublisher.Publish(new OpcUaReadOmegaTraceabilityRequestEvent());

        int elapsed = 0;
        while (!traceabilityComplete && elapsed < 5000)
        {
            await Task.Delay(100);
            elapsed += 100;
        }

        foreach (var (key, value) in spliceData)
        {
            traceabilityData.Add(new TraceabilityData { Key = key, Value = value });
        }

        try
        {
            await _client.TraceabilityPostAsync(new Traceability
            {
                ItemID = bundleId,
                OrderNumber = orderNumber,
                PartNumber = evt.ArticleName,
                CommProtocol = "Omega",
                Insertion = DateTime.UtcNow,
                Data = traceabilityData
            });
        }
        catch (ApiException ex) when (ex.StatusCode == 204)
        {
            OnUserInformation?.Invoke($"[Omega] Traceability stored", Severity.Success);
        }
    }
    catch (Exception ex)
    {
        OnUserInformation?.Invoke($"[Omega Error] {ex.Message}", Severity.Error);
    }
}
        public async Task<(bool success, string errorMessage)> WriteProductionInputNodesAsync(
            Bundle bundle,
            Func<long, Task<OrderMasterData>> masterDataLookupCallback,
            Action<string, Severity>? onUserInformation = null)
        {
            if (!IsOpcUaConfigured())
            {
                return (true, null);
            }

            if (bundle == null)
                return (false, "Bundle is null");

            try
            {
                onUserInformation?.Invoke(" Writing input nodes to OPC UA machine...", Severity.Info);

                var masterData = await masterDataLookupCallback(bundle.OrderNumber ?? 0);

                onUserInformation?.Invoke(" Fetching queue of bundles for this order...", Severity.Info);
                var allBundles = await _client.OrdersGetBundlesAsync(bundle.OrderNumber ?? 0);
                
                onUserInformation?.Invoke($"Static.status Found {(allBundles?.Count ?? 1)} bundles in queue", Severity.Info);
                
                var bomItems = new List<BomItem>();
                if (masterData?.Routing != null && bundle.CurrentStep > 0)
                {
                    var currentStepObj = masterData.Routing.FirstOrDefault(x => (int)x.Position == bundle.CurrentStep);
                    if (currentStepObj?.Stru != null)
                    {
                        bomItems = ((IEnumerable<dynamic>)currentStepObj.Stru)
                            .Select(item => new BomItem
                            {
                                PartNumber = (string)item.PartNumber,
                                Quantity = (int)item.Quantity
                            })
                            .ToList();
                    }
                }

                var bundleIdsList = allBundles?.Select(b => b.BundleID).ToList() ?? new List<string> { bundle.BundleID };
                
                await _opcUaClient.StartProductionWithOrderAsync(
                    bom: bomItems,
                    parameter: "",
                    partNumber: (string)(bundle.PartNumber ?? ""),
                    bundleId: bundle.BundleID,
                    bundleIdsInQueue: bundleIdsList,
                    quantity: (int)(bundle.Quantity ?? 1),
                    externalId: (string)(bundle.ExternalID ?? ""),
                    onStatusCallback: (msg) => onUserInformation?.Invoke(msg, Severity.Info)
                );

                onUserInformation?.Invoke("Static.status Input nodes written to machine", Severity.Success);
                return (true, null);
            }
            catch (Exception ex)
            {
                string errorMsg = $"Warning: Could not write OPC UA nodes: {ex.Message}";
                onUserInformation?.Invoke(errorMsg, Severity.Warning);
                return (false, errorMsg);
            }
        }

        private async Task<(ScanService.ExternalIdStatus status, string extractedId)> ValidateAndExtractExternalId(string value, string pattern = null)
        {
            try
            {
                var v = value?.Trim() ?? "";
                
                if (string.IsNullOrEmpty(v))
                    return (ScanService.ExternalIdStatus.Invalid, "");
                
                if (string.IsNullOrEmpty(pattern))
                    return (ScanService.ExternalIdStatus.Valid, v);

                var match = Regex.Match(v, pattern);
                if (!match.Success && (pattern.Contains("^") || pattern.Contains("$")))
                {
                    string flexiblePattern = pattern.Replace("^", "").Replace("$", "");
                    match = Regex.Match(v, flexiblePattern);
                }

                if (!match.Success)
                    return (ScanService.ExternalIdStatus.Invalid, "");

                string extracted = match.Groups.Count > 1 ? match.Groups[1].Value : match.Value;

                if (string.IsNullOrEmpty(extracted))
                    return (ScanService.ExternalIdStatus.Invalid, "");

                try
                {
                    var bundle = await _client.DispatcherGetBundleExternalAsync(extracted);
                    return bundle != null ? (ScanService.ExternalIdStatus.AlreadyExists, extracted) : (ScanService.ExternalIdStatus.Valid, extracted);
                }
                catch (ApiException ex) when (ex.StatusCode == 204)
                {
                    return (ScanService.ExternalIdStatus.Valid, extracted);
                }
                catch
                {
                    return (ScanService.ExternalIdStatus.Invalid, "");
                }
            }
            catch
            {
                return (ScanService.ExternalIdStatus.Invalid, "");
            }
        }
        async Task<(bool success, string error)> IOpcUaService.StartOmegaFeedbackMonitoringAsync(Bundle bundle, Workplace workplace, List<Resource> currentResources)
        {
            try
            {
                return await StartOmegaFeedbackMonitoringAsync(
                    bundle,
                    workplace,
                    currentResources);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }
        }
        async Task<(bool success, string error)> IOpcUaService.WriteProductionInputNodesAsync(
            Bundle bundle,
            Func<long, Task<OrderMasterData>> getMasterDataCallback,
            Action<string, int> onInformation)
        {
            try
            {
                Func<long, Task<OrderMasterData>> wrappedCallback = async (orderNumber) =>
                {
                    var result = await getMasterDataCallback(orderNumber);
                    return result;
                };

                Action<string, Severity> wrappedAction = (msg, severity) =>
                {
                    onInformation?.Invoke(msg, (int)severity);
                };

                var (success, errorMessage) = await WriteProductionInputNodesAsync(
                    bundle,
                    wrappedCallback,
                    wrappedAction);

                return (success, errorMessage);
            }
            catch (Exception ex)
            {
                return (false, ex.Message);
            }
        }
        async Task IOpcUaService.StopMonitoringAsync()
        {
            try
            {
                await StopCheckingResponseMonitoringAsync();
            }
            catch (Exception ex)
            {
                OnUserInformation?.Invoke($"Error stopping monitoring: {ex.Message}", Severity.Error);
            }
        }
         void IOpcUaService.Dispose()
        {
        }
    }
}
