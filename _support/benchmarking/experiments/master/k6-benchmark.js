import { Client, Stream, StatusOK } from 'k6/net/grpc'
import encoding from 'k6/encoding'
import { check } from 'k6'
import exec from 'k6/x/exec'

// Consume the environment variables we set in the Ansible task.
const gitalyAddress = __ENV.GITALY_ADDRESS
const gitalyProtoDir = __ENV.GITALY_PROTO_DIR
const runName = __ENV.RUN_NAME
const workloadDuration = __ENV.WORKLOAD_DURATION


// optionsStatic returns a test scenario where constant load is offered to Gitaly
const optionsStatic = () => {
  const SCENARIO_DEFAULTS = {
    executor: 'constant-arrival-rate',
    duration: workloadDuration,
    timeUnit: '1s',
    gracefulStop: '0s',
    preAllocatedVUs: 40
  }

  return {
    scenarios: {
      findCommit:         { ...SCENARIO_DEFAULTS, rate: 90, exec: 'findCommit' },
      listCommitsByOid:   { ...SCENARIO_DEFAULTS, rate: 90, exec: 'listCommitsByOid' },
      getBlobs:           { ...SCENARIO_DEFAULTS, rate: 90, exec: 'getBlobs' },
      getTreeEntries:     { ...SCENARIO_DEFAULTS, rate: 90, exec: 'getTreeEntries' },
      treeEntry:          { ...SCENARIO_DEFAULTS, rate: 40, exec: 'treeEntry' },
      writeAndDeleteRefs: { ...SCENARIO_DEFAULTS, rate: 1, exec: 'writeAndDeleteRefs' },
      userCommitFiles:    { ...SCENARIO_DEFAULTS, rate: 2, exec: 'userCommitFiles' },
      userMergeBranch:    { ...SCENARIO_DEFAULTS, rate: 1, exec: 'userMergeBranch' },
    },
    setupTimeout: '5m'
  }
}

// optionsRamping returns a test scenario where a ramping workload is offered to Gitaly
const optionsRamping = () => {
  const SCENARIO_DEFAULTS = {
    executor: 'ramping-arrival-rate',
    timeUnit: '1s',
    preAllocatedVUs: 40
  }

  const stages_read = [{target: 50, duration: '20s'}, {target: 100, duration: '10s'}, {target: 200, duration: '20s'}, {target: 50, duration: '10s'}]
  const stages_write = [{target: 25, duration: '20s'}, {target: 50, duration: '10s'}, {target: 100, duration: '20s'}, {target: 25, duration: '10s'}]

  return {
    scenarios: {
      findCommit: {
        ...SCENARIO_DEFAULTS,
        stages: stages_read,
        exec: 'findCommit'
      },
      getBlobs: {
        ...SCENARIO_DEFAULTS,
        stages: stages_read,
        exec: 'getBlobs'
      },
      getTreeEntries: {
        ...SCENARIO_DEFAULTS,
        stages: stages_read,
        exec: 'getTreeEntries'
      },
      treeEntry: {
        ...SCENARIO_DEFAULTS,
        stages: stages_read,
        exec: 'treeEntry'
      },
      listCommitsByOid: {
        ...SCENARIO_DEFAULTS,
        stages: stages_read,
        exec: 'listCommitsByOid'
      },
      writeAndDeleteRefs: {
        ...SCENARIO_DEFAULTS,
        stages: stages_write,
        exec: 'writeAndDeleteRefs'
      },
      userCommitFiles: {
        ...SCENARIO_DEFAULTS,
        stages: stages_write,
        exec: 'userCommitFiles'
      },
      userMergeBranch: {
        ...SCENARIO_DEFAULTS,
        stages: stages_write,
        exec: 'userMergeBranch'
      },
    },
    setupTimeout: '5m'
  }

}

export const options = optionsStatic()

const repos = JSON.parse(open("/opt/benchmark-gitaly/repositories.json"));

const selectTestRepo = () => {
  const active = repos.filter(r => r.include_in_test);
  const repo = active[Math.floor(Math.random() * active.length)];

  return {
    repository: {
      storageName: 'default',
      relativePath: `${repo.name}`,
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

const teardownClient = new Client()
teardownClient.load([gitalyProtoDir], 'ref.proto')

export function teardown (context) {
  console.log('Teardown: cleaning up benchmark branches...')
  const testRepos = repos.filter(r => r.include_in_test)
  const totalRepos = testRepos.length

  let currentRepo = 0
  teardownClient.connect(gitalyAddress, {
    plaintext: true
  })

  for (const repo of testRepos) {
    const repository = {
      storageName: 'default',
      relativePath: repo.name,
      glRepository: repo.name,
      glProjectPath: `foo/bar/${repo.name}`,
    }

    const stream = new Stream(teardownClient, 'gitaly.RefService/FindLocalBranches')
    const branchesToDelete = []

    stream.on('data', data => {
      if (data.localBranches) {
        for (const branch of data.localBranches) {
          const branchName = encoding.b64decode(branch.name, 'std', 's')
          if (branchName.startsWith('refs/heads/benchmark-')) {
            branchesToDelete.push(branchName)
          }
        }
      }
    })

    stream.on('end', function () {
      currentRepo++
      if (branchesToDelete.length > 0) {
        console.log(`Found ${branchesToDelete.length} branches to delete`)
        const BATCH_SIZE = 100
        const batches = []
        for (let i = 0; i < branchesToDelete.length; i += BATCH_SIZE) {
          batches.push(branchesToDelete.slice(i, i + BATCH_SIZE))
        }

        for (let i = 0; i < batches.length; i++) {
          const deleteReq = {
            repository: repository,
            refs: batches[i].map(name => encoding.b64encode(name))
          }

          const deleteRes = teardownClient.invoke('gitaly.RefService/DeleteRefs', deleteReq)
          check(deleteRes, {
            'DeleteRefs - StatusOK': r => r && r.status === StatusOK
          })
        }
        console.log(`Completed ${currentRepo} of ${totalRepos} repositories`)
        if (currentRepo === totalRepos) {
          console.log('Teardown: all repositories cleaned')
          teardownClient.close()
        }

      } else {
        console.log(`No benchmark branches found`)
      }
    })

    stream.on('error', function(err) {
      console.error(`Error cleaning ${repo.name}:`, err)
    })

    stream.write({ repository: repository })
  }

  exec.command('rm', [context.setupCompletionSentinel])
}

const client = new Client()
// k6 provides no easy way to list directory contents.
client.load([gitalyProtoDir], 'commit.proto', 'blob.proto', 'ref.proto', 'repository.proto', 'operations.proto')

export function findCommit () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const req = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.commit)
  }

  const res = client.invoke('gitaly.CommitService/FindCommit', req)
  check(res, {
    'FindCommit - StatusOK': r => r && r.status === StatusOK
  })

  client.close()
}

export function getBlobs () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const req = {
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
      'GetBlobs - data present in response': r => r && r.data
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(req)
}

export function getTreeEntries () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const req = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.commit),
    path: encoding.b64encode(testRepo.directory)
  }

  const stream = new Stream(client, 'gitaly.CommitService/GetTreeEntries')
  stream.on('data', data => {
    check(data, {
      'GetTreeEntries - entries present in response': r => r && r.entries
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(req)
}

export function treeEntry () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const req = {
    repository: testRepo.repository,
    revision: encoding.b64encode(testRepo.ref),
    path: encoding.b64encode(testRepo.file)
  }

  const stream = new Stream(client, 'gitaly.CommitService/TreeEntry')
  stream.on('data', data => {
    check(data, {
      'TreeEntry - data present in response': r => r && r.data
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(req)
}

export function listCommitsByOid () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const req = {
    repository: testRepo.repository,
    oid: [testRepo.commit]
  }

  const stream = new Stream(client, 'gitaly.CommitService/ListCommitsByOid')
  stream.on('data', data => {
    check(data, {
      'ListCommitsByOid - commits present in response': r => r && r.commits
    })
  })

  stream.on('end', function () {
    client.close()
  })

  stream.on('error', function(err) {
    console.error(err)
  })

  stream.write(req)
}

export function writeAndDeleteRefs () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const generatedRef = 'refs/test/' + generateRandom()

  const writeRefReq = {
    repository: testRepo.repository,
    ref: encoding.b64encode(generatedRef),
    revision: encoding.b64encode(testRepo.commit)
  }

  const writeRefRes = client.invoke('gitaly.RepositoryService/WriteRef', writeRefReq)
  check(writeRefRes, {
    'WriteRef - StatusOK': r => r && r.status === StatusOK
  })

  const deleteRefsReq = {
    repository: testRepo.repository,
    refs: [encoding.b64encode(generatedRef)]
  }

  const deleteRefsRes = client.invoke('gitaly.RefService/DeleteRefs', deleteRefsReq)
  check(deleteRefsRes, {
    'DeleteRefs - StatusOK': r => r && r.status === StatusOK
  })

  client.close()
}

export function userCommitFiles () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const branchName = 'benchmark-commit-' + generateRandom()
  const fileName = `benchmark-${generateRandom()}.txt`

  const stream = new Stream(client, 'gitaly.OperationService/UserCommitFiles')

  let responseReceived = false
  stream.on('data', data => {
    responseReceived = true
    check(data, {
      'UserCommitFiles - branch_update returned': r => r && r.branchUpdate
    })
  })

  stream.on('end', function () {
    check(responseReceived, {
      'UserCommitFiles - received response': r => r === true
    })
    client.close()
  })

  stream.on('error', function(err) {
    console.error('UserCommitFiles error:', err)
    client.close()
  })

  stream.write({
    header: {
      repository: testRepo.repository,
      user: {
        glId: 'user-1',
        name: encoding.b64encode('Benchmark User'),
        email: encoding.b64encode('benchmark@example.com'),
        glUsername: 'benchmark'
      },
      branchName: encoding.b64encode(branchName),
      startBranchName: encoding.b64encode('master'),
      commitMessage: encoding.b64encode('Benchmark commit')
    }
  })

  // action header
  stream.write({
    action: {
      header: {
        action: 'CREATE',
        filePath: encoding.b64encode(fileName),
        base64Content: false
      }
    }
  })

  // action content
  stream.write({
    action: {
      content: encoding.b64encode('Benchmark file content for testing')
    }
  })

  stream.end()
}

export function userMergeBranch () {
  client.connect(gitalyAddress, {
    plaintext: true
  })

  const testRepo = selectTestRepo();
  const sourceBranch = 'benchmark-source-' + generateRandom()
  const targetBranch = 'benchmark-target-' + generateRandom()

  // Create unique target branch from master to avoid race conditions
  const createTargetReq = {
    repository: testRepo.repository,
    ref: encoding.b64encode(`refs/heads/${targetBranch}`),
    revision: encoding.b64encode('master')
  }
  const writeRefRes = client.invoke('gitaly.RepositoryService/WriteRef', createTargetReq)
  check(writeRefRes, {
    'WriteRef - StatusOK': r => r && r.status === StatusOK
  })

  // Create a new commit with master as parent
  const commitStream = new Stream(client, 'gitaly.OperationService/UserCommitFiles')

  let newCommitId = null

  commitStream.on('data', data => {
    if (data.branchUpdate) {
      newCommitId = data.branchUpdate.commitId
    }
  })

  commitStream.on('end', function () {
    const mergeStream = new Stream(client, 'gitaly.OperationService/UserMergeBranch')

    let messagesReceived = 0

    mergeStream.on('data', data => {
      messagesReceived++
      if (messagesReceived === 1) {
        check(data, {
          'UserMergeBranch - commit_id returned': r => r && r.commitId
        })
        mergeStream.write({ apply: true })
      } else {
        check(data, {
          'UserMergeBranch - branch_update returned': r => r && r.branchUpdate
        })
      }
    })

    mergeStream.on('end', function () {
      check(messagesReceived, {
        'UserMergeBranch - received both responses': r => r === 2
      })
      client.close()
    })

    mergeStream.on('error', function(err) {
      console.error('UserMergeBranch error:', err)
      client.close()
    })

    mergeStream.write({
      repository: testRepo.repository,
      user: {
        glId: 'user-1',
        name: encoding.b64encode('Benchmark User'),
        email: encoding.b64encode('benchmark@example.com'),
        glUsername: 'benchmark'
      },
      commitId: newCommitId,
      branch: encoding.b64encode(targetBranch),
      message: encoding.b64encode('Benchmark merge')
    })
  })

  commitStream.on('error', function(err) {
    console.error('UserCommitFiles error:', err)
    client.close()
  })

  // Create commit with master as parent
  commitStream.write({
    header: {
      repository: testRepo.repository,
      user: {
        glId: 'user-1',
        name: encoding.b64encode('Benchmark User'),
        email: encoding.b64encode('benchmark@example.com'),
        glUsername: 'benchmark'
      },
      branchName: encoding.b64encode(sourceBranch),
      startBranchName: encoding.b64encode('master'),
      commitMessage: encoding.b64encode('Benchmark commit for merge')
    }
  })

  commitStream.write({
    action: {
      header: {
        action: 'CREATE',
        filePath: encoding.b64encode(`benchmark-${generateRandom()}.txt`),
        base64Content: false
      }
    }
  })

  commitStream.write({
    action: {
      content: encoding.b64encode('Benchmark content')
    }
  })

  commitStream.end()
}
