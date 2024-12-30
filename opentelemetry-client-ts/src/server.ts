import express from 'express'
import { initializeTracing } from './tracing'
import { trace } from '@opentelemetry/api'

// Initialize OpenTelemetry
initializeTracing()

const app = express()
const port = 3015

// Middleware to parse JSON bodies
app.use(express.json())

// Routes
app.get('/hello', async (req, res) => {
  console.info('Handling /hello request')
  const span = trace.getActiveSpan()
  span?.setAttribute('hello-attribute', 'hello-value')
  await new Promise(resolve => setTimeout(resolve, 100))
  res.send('Hello, OpenTelemetry!')
})

app.get('/external', async (req, res) => {
  console.debug('Making external request to GitHub API')
  try {
    const response = await fetch('https://api.github.com/users/github')
    const data = await response.json()
    res.json(data)
  } catch (error) {
    console.warn('Error fetching from GitHub:', error)
    res.status(500).json({ error: 'Failed to fetch from GitHub' })
  }
})

// Test endpoint with multiple spans
app.get('/test-spans', async (req, res) => {
  console.info('Starting multiple span test')
  const span = trace.getActiveSpan()
  span?.setAttribute('test-attribute', 'test-value')
  span?.addEvent('test-event', { testEventAttribute: 'test-event-value' })
  // First delay
  await new Promise(resolve => setTimeout(resolve, 100))
  console.info('Completed first delay')
  
  // Second delay
  await new Promise(resolve => setTimeout(resolve, 100))
  console.info('Completed second delay')
  
  // Third delay
  await new Promise(resolve => setTimeout(resolve, 100))
  console.info('Completed third delay')
  
  res.json({ message: 'Completed all delays', totalTime: '300ms' })
})

// 404 handler
app.use((req, res) => {
  console.warn(`Route not found: ${req.url}`)
  res.status(404).send('Not Found')
})

// Error handler
app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Server error:', err)
  res.status(500).send('Internal Server Error')
})

app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`)
}) 