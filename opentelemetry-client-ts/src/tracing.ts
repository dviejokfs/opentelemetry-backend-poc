import { context, diag, DiagConsoleLogger, DiagLogLevel, trace } from '@opentelemetry/api'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { Resource } from '@opentelemetry/resources'
import { ConsoleLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from '@opentelemetry/sdk-logs'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions'

export function initializeTracing() {
	// Enable OpenTelemetry debug logging
	diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.ERROR)

	const commonResource = new Resource({
		[SemanticResourceAttributes.SERVICE_NAME]: 'my-bun-service',
	})

	// Create logger provider
	const loggerProvider = new LoggerProvider({
		resource: commonResource,
	})

	// Add OTLP exporter for logs
	loggerProvider.addLogRecordProcessor(
		new SimpleLogRecordProcessor(
			new OTLPLogExporter({
				url: 'http://localhost:4000/v1/logs',
				timeoutMillis: 30000,
			})
		)
	)

	// Console exporter for logs
	loggerProvider.addLogRecordProcessor(
		new SimpleLogRecordProcessor(new ConsoleLogRecordExporter())
	)

	const sdk = new NodeSDK({
		resource: commonResource,
		traceExporter: new OTLPTraceExporter({
			url: 'http://localhost:4000/v1/traces',
			timeoutMillis: 30000,
		}),
		spanProcessor: new BatchSpanProcessor(
			new OTLPTraceExporter({
				url: 'http://localhost:4000/v1/spans',
				timeoutMillis: 30000,
			})
		),
		instrumentations: [
			getNodeAutoInstrumentations({
				'@opentelemetry/instrumentation-http': {
					enabled: true,
				},
				'@opentelemetry/instrumentation-express': {
					enabled: true,
				},
			}),
		],
	})

	const originalConsole = {
		log: console.log,
		error: console.error,
		warn: console.warn,
		info: console.info,
	}

	// Get a tracer and logger
	const tracer = trace.getTracer('console-instrumentation')
	const logger = loggerProvider.getLogger('console-logger')

	// Override console methods to use both tracing and logging
	console.log = (...args: any[]) => {
		const span = tracer.startSpan('console.log')
		const ctx = trace.setSpan(context.active(), span)

		context.with(ctx, () => {
			logger.emit({
				severityText: 'INFO',
				body: args.map((arg) => String(arg)).join(' '),
				attributes: {
					'log.type': 'console.log',
				},
			})
			originalConsole.log(...args)
		})

		span.end()
	}

	// console.error = (...args: any[]) => {
	// 	const span = tracer.startSpan('console.error')
	// 	const ctx = trace.setSpan(context.active(), span)

	// 	context.with(ctx, () => {
	// 		logger.emit({
	// 			severityText: 'ERROR',
	// 			body: args.map(arg => String(arg)).join(' '),
	// 			attributes: {
	// 				'log.type': 'console.error',
	// 			},
	// 		})
	// 		originalConsole.error(...args)
	// 	})

	// 	span.end()
	// }

	console.warn = (...args: any[]) => {
		const span = tracer.startSpan('console.warn')
		const ctx = trace.setSpan(context.active(), span)

		context.with(ctx, () => {
			logger.emit({
				severityText: 'WARN',
				body: args.map((arg) => String(arg)).join(' '),
				attributes: {
					'log.type': 'console.warn',
				},
			})
			originalConsole.warn(...args)
		})

		span.end()
	}

	console.info = (...args: any[]) => {
		const span = tracer.startSpan('console.info')
		const ctx = trace.setSpan(context.active(), span)

		context.with(ctx, () => {
			logger.emit({
				severityText: 'INFO',
				body: args.map((arg) => String(arg)).join(' '),
				attributes: {
					'log.type': 'console.info',
				},
			})
			originalConsole.info(...args)
		})

		span.end()
	}

	// Start the SDK
	sdk.start()

	// Graceful shutdown
	process.on('SIGTERM', () => {
		Promise.all([sdk.shutdown(), loggerProvider.shutdown()])
			.then(() => console.log('Tracing and logging terminated'))
			.catch((error) => console.log('Error terminating tracing and logging', error))
			.finally(() => process.exit(0))
	})
}
