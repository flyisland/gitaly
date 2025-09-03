import { Client, grpc } from 'k6/net/grpc';
import encoding from 'k6/encoding';
import { check } from 'k6';
import exec from "k6/x/exec";

// Consume the environment variables we set in the Ansible task.
const gitalyAddress = __ENV.GITALY_ADDRESS;
const gitalyProtoDir = __ENV.GITALY_PROTO_DIR;
const runName = __ENV.RUN_NAME;
const workloadDuration = __ENV.WORKLOAD_DURATION;

export const options = {
    scenarios: {
        findCommit: {
            executor: 'constant-arrival-rate',
            duration: workloadDuration,
            timeUnit: '1s',
            rate: 100,
            gracefulStop: '0s',
            preAllocatedVUs: 10,
            exec: "findCommit",
        }
    },
    setupTimeout: '5m'
}

export function setup() {
    const setupCompletionSentinel = `/tmp/${runName}-setup-complete`;
    // Signal to Ansible that setup is complete, in a very hacky way.
    exec.command("touch", [setupCompletionSentinel])

    return {
        setupCompletionSentinel
    }
}

export function teardown(context) {
    exec.command("rm", [context.setupCompletionSentinel]);
}

const client = new Client();
// k6 provides no easy way to list directory contents.
client.load([gitalyProtoDir], 'commit.proto');

export function findCommit() {
    client.connect(gitalyAddress, {
        plaintext: true,
    });

    const data = {
        "repository": {
            "storageName": "default",
            "relativePath": "git.git",
            "gitAlternateObjectDirectories": [],
            "glRepository": "git",
            "glProjectPath": "gitlab-org/git"
        },
        "revision": encoding.b64encode("master")
    }
    const response = client.invoke('gitaly.CommitService/FindCommit', data);
}

