namespace EasyNetQ.PollyHandlerRunner {
    using System;
    using System.Threading.Tasks;
    using EasyNetQ;
    using EasyNetQ.Consumer;
    using EasyNetQ.Internals;
    using EasyNetQ.Logging;
    using Polly;

    /// <summary>
    /// An implementation of <see cref="HandlerRunner"/> which executes message consumers within a Polly policy.
    /// </summary>
    public class PollyHandlerRunner : HandlerRunner {
        readonly ILog _logger;
        readonly Policy _policy;

        /// <summary>
        /// Initializes a new instance of the <see cref="PollyHandlerRunner"/> class.
        /// </summary>
        /// <param name="logger">A reference to an EasyNetQ logger implementation.</param>
        /// <param name="consumerErrorStrategy">A reference to a consumer error strategy.</param>
        /// <param name="policy">A reference to the policy within which message consumers will be executed.</param>
        public PollyHandlerRunner(ILog logger, IConsumerErrorStrategy consumerErrorStrategy, Policy policy)
            : base(consumerErrorStrategy) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        }

        /// <inheritdoc />
        public override Task<AckStrategy> InvokeUserMessageHandlerAsync(ConsumerExecutionContext context) {
            _logger.Debug("Received \n\tRoutingKey: '{0}'\n\tCorrelationId: '{1}'\n\tConsumerTag: '{2}'\n\tDeliveryTag: {3}\n\tRedelivered: {4}",
                context.Info.RoutingKey,
                context.Properties.CorrelationId,
                context.Info.ConsumerTag,
                context.Info.DeliverTag,
                context.Info.Redelivered);

            Task completionTask;

            try {
                completionTask = _policy.Execute(() => {
                    var task = context.UserHandler(context.Body, context.Properties, context.Info);

                    if (task.IsFaulted)
                        throw task.Exception.GetBaseException();

                    return task;
                });
            }
            catch (Exception exception) {
                completionTask = Task.FromException(exception);
            }

            if (completionTask.Status == TaskStatus.Created) {
                _logger.Error("Task returned from consumer callback is not started. ConsumerTag: '{0}'", context.Info.ConsumerTag);
                return null;
            }

            return completionTask.ContinueWith(task => AckStrategies.Ack);
        }
      }
}
