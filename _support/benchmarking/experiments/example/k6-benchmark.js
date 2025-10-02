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
      findCommit: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'findCommit' },
      getBlobs: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'getBlobs' },
      getTreeEntries: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'getTreeEntries' },
      treeEntry: { ...SCENARIO_DEFAULTS, rate: 100, exec: 'treeEntry' },
      listCommitsByOid: { ...SCENARIO_DEFAULTS, rate: 200, exec: 'listCommitsByOid' },
      writeAndDeleteRefs: { ...SCENARIO_DEFAULTS, rate: 100, exec: 'writeAndDeleteRefs' }
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
      }
    },
    setupTimeout: '5m'
  }

}

export const options = optionsRamping()

const repos = JSON.parse(open("/opt/benchmark-gitaly/repositories.json"));

const selectTestRepo = () => {
  const active = repos.filter(r => r.include_in_test);
  const repo = active[Math.floor(Math.random() * active.length)];

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
