import exec from 'k6/x/exec'

const gitalyAddress = __ENV.GITALY_ADDRESS
const runName = __ENV.RUN_NAME
const workloadDuration = __ENV.WORKLOAD_DURATION
const fetcherConcurrency = __ENV.FETCHER_CONCURRENCY || '10'
const fetcherDuration = __ENV.FETCHER_DURATION || workloadDuration
const fetcherMode = __ENV.FETCHER_MODE || 'incremental'

export const options = {
  scenarios: {
    fetcher: {
      executor: 'shared-iterations',
      iterations: 1,
      vus: 1,
    }
  },
  setupTimeout: '1m',
}

export function setup () {
  exec.command('touch', [`/tmp/${runName}-setup-complete`])
}

export default function () {
  exec.command('fetcher', [
    `-addr=${gitalyAddress}`,
    `-repos=/opt/benchmark-gitaly/repositories.json`,
    `-concurrency=${fetcherConcurrency}`,
    `-duration=${fetcherDuration}`,
    `-mode=${fetcherMode}`,
  ])
}
