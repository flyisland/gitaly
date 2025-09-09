import { Client, Stream, StatusOK } from 'k6/net/grpc'
import encoding from 'k6/encoding'
import { check } from 'k6'
import exec from 'k6/x/exec'

// Consume the environment variables we set in the Ansible task.
const gitalyAddress = __ENV.GITALY_ADDRESS
const gitalyProtoDir = __ENV.GITALY_PROTO_DIR
const runName = __ENV.RUN_NAME
const workloadDuration = __ENV.WORKLOAD_DURATION

const SCENARIO_DEFAULTS = {
  executor: 'constant-arrival-rate',
  duration: workloadDuration,
  timeUnit: '1s',
  gracefulStop: '0s',
  preAllocatedVUs: 40
}

export const options = {
  scenarios: {
    findCommit: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'findCommit' },
    getBlobs: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'getBlobs' },
    getTreeEntries: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'getTreeEntries' },
    treeEntry: { ...SCENARIO_DEFAULTS, rate: 100, exec: 'treeEntry' },
    listCommitsByOid: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'listCommitsByOid' },
    writeAndDeleteRefs: { ...SCENARIO_DEFAULTS, rate: 100, exec: 'writeAndDeleteRefs' }
  },
  setupTimeout: '5m'
}

const repos = JSON.parse(open("/opt/benchmark-gitaly/repositories.json"));

const selectTestRepo = () => {
  const repo = repos[Math.floor(Math.random() * repos.length)];

  return {
    repository: {
      storageName: 'default',
      relativePath: `${repo.name}.git`,
      glRepository: repo.name,                // irrelevant but mandatory
      glProjectPath: `foo/bar/${repo.name}`,  // irrelevant but mandatory
    },
    commit: repo.testdata.commits[Math.floor(Math.random() * repo.testdata.commits.length)],
    ref: repo.testdata.refs[Math.floor(Math.random() * repo.testdata.refs.length)],
    file: repo.testdata.files[Math.floor(Math.random() * repo.testdata.files.length)],
    directory: repo.testdata.directories[Math.floor(Math.random() * repo.testdata.directories.length)],
  }
}

const generateRandom = () => Math.random().toString(36).substring(2, 15) + Math.random().toString(23).substring(2, 5)

export function setup () {
  const setupCompletionSentinel = `/tmp/${runName}-setup-complete`
  // Signal to Ansible that setup is complete, in a very hacky way.
  exec.command('touch', [setupCompletionSentinel])

  return {
    setupCompletionSentinel
  }
}

export function teardown (context) {
  exec.command('rm', [context.setupCompletionSentinel])
}

const client = new Client()
// k6 provides no easy way to list directory contents.
client.load([gitalyProtoDir], 'commit.proto', 'blob.proto', 'ref.proto', 'repository.proto')

export function findCommit () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const data = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.commit)
  }

  const response = client.invoke('gitaly.CommitService/FindCommit', data)
  check(response, {
    'findCommit status is OK': r => r && r.status === StatusOK
  })

  client.close()
}

export function getBlobs () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const getBlobsRequest = {
    repository: testRepo.repository,
    revision_paths: [
      {
        revision: testRepo.commit,
        path: encoding.b64encode(testRepo.file)
      }
    ],
    limit: -1
  }

  const stream = new Stream(client, 'gitaly.BlobService/GetBlobs')
  stream.on('data', data => {
    check(data, {
      'type is BLOB': r => r && r.type === 'BLOB'
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(getBlobsRequest)
}

export function getTreeEntries () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const getTreeEntriesRequest = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.commit),
    path: encoding.b64encode(testRepo.directory)
  }

  const stream = new Stream(client, 'gitaly.CommitService/GetTreeEntries')
  stream.on('data', data => {
    check(data, {
      'entries exists in GetTreeEntriesResponse': r => r && r.entries
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(getTreeEntriesRequest)
}

export function treeEntry () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const treeEntryRequest = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.ref),
    path: encoding.b64encode(testRepo.file)
  }

  const stream = new Stream(client, 'gitaly.CommitService/TreeEntry')
  stream.on('data', data => {
    check(data, {
      'data exists in TreeEntryResponse': r => r && r.data
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(treeEntryRequest)
}

export function listCommitsByOid () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const listCommitsByOidRequest = {
    repository: testRepo.repository,
    oid: [testRepo.commit]
  }

  const stream = new Stream(client, 'gitaly.CommitService/ListCommitsByOid')
  stream.on('data', data => {
    check(data, {
      'commits exists in listCommitsByOid': r => r && r.commits
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(listCommitsByOidRequest)
}

export function writeAndDeleteRefs () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();

  const generatedRef = 'refs/test/' + generateRandom()

  const data = {
    repository: testRepo.repository,
    ref: encoding.b64encode(generatedRef),
    revision: encoding.b64encode(testRepo.commit)
  }
  const response = client.invoke('gitaly.RepositoryService/WriteRef', data)
  check(response, {
    'WriteRef status is OK': r => r && r.status === StatusOK
  })

  const deleteRefData = {
    repository: testRepo.repository,
    refs: [encoding.b64encode(generatedRef)]
  }

  const deleteRefResponse = client.invoke('gitaly.RefService/DeleteRefs', deleteRefData)
  check(deleteRefResponse, {
    'DeleteRefs status is OK': r => r && r.status === StatusOK
  })

  client.close()
}
