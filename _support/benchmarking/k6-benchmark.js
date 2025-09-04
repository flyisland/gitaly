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

// Test repository configuration
const testRepo = {
  storageName: 'default',
  relativePath: 'git.git',
  gitAlternateObjectDirectories: [],
  glRepository: 'git',
  glProjectPath: 'gitlab-org/git'
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
  try {
    client.connect(gitalyAddress, {
      plaintext: true
    })

    const data = {
      repository: testRepo,
      revision: encoding.b64encode('master')
    }
    const response = client.invoke('gitaly.CommitService/FindCommit', data)
    check(response, {
      'findCommit status is OK': r => r && r.status === StatusOK
    })

    console.log(JSON.stringify(response.message))
  } catch (error) {
    console.log(`[FindCommit] ✗ Error: ${error.message || error}`)
  } finally {
    if (client) {
      client.close()
    }
  }
}

export function getBlobs () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const getBlobsRequest = {
    repository: testRepo,
    revision_paths: [
      {
        revision: 'master',
        path: encoding.b64encode('README.md')
      }
    ],
    limit: -1
  }

  const stream = new Stream(client, 'gitaly.BlobService/GetBlobs')
  stream.on('data', data => {
    check(data, {
      'type is BLOB': r => r && r.type === 'BLOB'
    })

    console.log('Received message from GetBlobs: ', JSON.stringify(data))
  })

  stream.on('end', function () {
    // The server has finished sending
    client.close()
  })

  // send a message to the server
  stream.write(getBlobsRequest)
}

export function getTreeEntries () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const getTreeEntriesRequest = {
    repository: testRepo,
    revision: encoding.b64encode('master'),
    path: encoding.b64encode('Documentation')
  }

  const stream = new Stream(client, 'gitaly.CommitService/GetTreeEntries')
  stream.on('data', data => {
    check(data, {
      'entries exists in GetTreeEntriesResponse': r => r && r.entries
    })

    console.log('Received message from GetTreeEntries: ', JSON.stringify(data))
  })

  stream.on('end', function () {
    // The server has finished sending
    client.close()
  })

  // send a message to the server
  stream.write(getTreeEntriesRequest)
}

export function treeEntry () {
  client.connect(gitalyAddress, {
    plaintext: true
  })
  const treeEntryRequest = {
    repository: testRepo,
    revision: encoding.b64encode('master'),
    path: encoding.b64encode('templates/Makefile')
  }

  const stream = new Stream(client, 'gitaly.CommitService/TreeEntry')
  stream.on('data', data => {
    check(data, {
      'data exists in TreeEntryResponse': r => r && r.data
    })

    console.log('Received message from TreeEntry: ', JSON.stringify(data))
  })

  stream.on('end', function () {
    // The server has finished sending
    client.close()
  })

  // send a message to the server
  stream.write(treeEntryRequest)
}

export function listCommitsByOid () {
  client.connect(gitalyAddress, {
    plaintext: true
  })
  const listCommitsByOidRequest = {
    repository: testRepo,
    oid: ['4ff55cf24f68ee90e73de04f823c36bf536882bd']
  }

  const stream = new Stream(client, 'gitaly.CommitService/ListCommitsByOid')
  stream.on('data', data => {
    check(data, {
      'commits exists in listCommitsByOid': r => r && r.commits
    })

    console.log('Received message from listCommitsByOid: ', JSON.stringify(data))
  })

  stream.on('end', function () {
    // The server has finished sending
    client.close()
  })

  // send a message to the server
  stream.write(listCommitsByOidRequest)
}

export function writeAndDeleteRefs () {
  try {
    client.connect(gitalyAddress, {
      plaintext: true
    })

    const generatedRef = 'refs/test/' + generateRandom()

    const data = {
      repository: testRepo,
      ref: encoding.b64encode(generatedRef),
      revision: encoding.b64encode('8b6f19ccfc3aefbd0f22f6b7d56ad6a3fc5e4f37')
    }
    const response = client.invoke('gitaly.RepositoryService/WriteRef', data)
    check(response, {
      'WriteRef status is OK': r => r && r.status === StatusOK
    })

    console.log(JSON.stringify(response.message))

    const deleteRefData = {
      repository: testRepo,
      refs: [encoding.b64encode(generatedRef)]
    }

    const deleteRefResponse = client.invoke('gitaly.RefService/DeleteRefs', deleteRefData)
    check(deleteRefResponse, {
      'DeleteRefs status is OK': r => r && r.status === StatusOK
    })

    console.log(JSON.stringify(deleteRefResponse.message))
    if (deleteRefResponse.status !== StatusOK) {
      console.log('DeleteRefs failed with error: ', deleteRefResponse.error)
    }
  } catch (error) {
    console.log(`[DeleteRefs] ✗ Error: ${error.message || error}`)
  } finally {
    if (client) {
      client.close()
    }
  }
}
