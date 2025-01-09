# Rhino

> ⚠️ Rhino is a PoC, experimental software. Don't use in production. ⚠️

Key-value database for low-latency edge workloads built on top of FoundationDB.

## Talks

Rhino was presented at RustLab 2024. You can watch the recording [here](https://youtu.be/9B_v6CDFf-Y?si=WXXhPzcKf4BGN-iy).

## How Rhino is different from Turso?

In terms of edge databases in general, [Turso](https://turso.tech/) is definitely the main competitor to Rhino. There are, however, three key differences:

1. Turso focuses on providing raw SQLite interface
    - Rhino is a NoSQL database, which not only provides flexibility to developers, but also allows us to provide richer, more expressive APIs such as queues and new index types which are not present in a typical SQL database.
2. Turso is not ready for interactive transactional workloads
    - Turso does support interactive transactions, but considers them an anti-pattern and *locks the entire database for the duration of each transaction*.
    - Rhino is specifically designed to provide interactive transactions at low latency and without any locking.
3. Turso still attempts to give the developer an illusion of one big global database.
    - All databases of Turso are a part of a "database group". Database group has a fixed primary location, which cannot be changed. This means that if you want to put your primary (which is responsible for all writes) close to the user, you need multiple database groups in various locations. The number of these groups is severely limited on all plans but the enterprise - which is a signal to me that they do not expect a typical user to have multiple groups. Even worse, you cannot control the primary location of a group from the SDK - you need to manually use their CLI or their raw Platform API - another signal that they do not expect this behaviour to be common. This all means that they basically want the developer to pick a couple of key locations and suffer from latency on all writes if the user is not close to them.
    - Rhino allows the developer to pick the storage location for each small logical database they create, right from the SDK. This means that the user's data will always be stored in the location closest to them.

## Various servers of Rhino

Rhino consists of 4 types of servers.

- **API server** A WebSocket server which users connect to. This server is designed to handle external traffic, so it is safe to expose it to the internet. API server never connects to FDB clusters directly and performs all operations through Region servers.
- **Region server** An HTTP server which exposes operations on a single FDB cluster. These operations include: add an auth token, create a shard, insert some rows into a table, etc. This server should never be exposed to the internet.
- **Control server** An HTTP server which exposes operations on a central FDB cluster. These operations include: add an auth token and replicate it to Region servers, mark shard as located in region X, etc. This is the source of truth about which API tokens are valid, table schema and where shards are located. This server should never be exposed to the internet.
- **Replicator** Replicates tokens and shard routing information from the central region to all other regions.

All servers are stateless (as in "all data is stored in FDB"). All operations are idempotent (I think).

## Starting servers of Rhino

```
./scripts/api_server.sh
./scripts/control_server.sh
./scripts/region_server.sh
./scripts/replicator.sh
```

## Setting up multiple FoundationDB clusters for development on macOS

You can do that with setup based on `docker-compose` like this:

```
cd configs/fdb
./setup.sh
```

After that, you should be able to use cluster files in the same directory to connect to different FDB clusters.

## Testing

Installing dependencies

```
pip3 install websocket-client
pip3 install python-ulid
pip3 install foundationdb==7.1.61
```

Running the tests

```
./test/start_all.sh
# Give servers the time to set up and establish connections to each other.
# Have a cup of tea while you are at it.
sleep 5

./tests/run.sh
```

## Setting up single FoundationDB for development on macOS

There is no official Docker image of FDB for arm64 architecture. The amd64 image fails under emulation with illegal instruction.

However, despite the official tutorial explicitly stating the need for amd64 CPU to install FDB on Mac, you can install it on arm64 using `.pkg` installer from [the releases page](https://github.com/apple/foundationdb/releases/tag/7.1.61) on Github.

Once you install FDB, reboot the machine. Otherwise, connections to FDB will fail with "unreachable" error. After that, the server should be running in background with cluster file located in `/usr/local/etc/foundationdb/fdb.cluster`. There will be also `fdbcli` command line tool available.

Finally, you need to create the database. Run `fdbcli`, which will open an interactive shell for admin commands. After that, run the following command:

```
fdb> configure new single ssd
Database created
```

Check the status of the newly created database with:

```
fdb> status

Using cluster file `/usr/local/etc/foundationdb/fdb.cluster'.

Configuration:
  Redundancy mode        - single
  Storage engine         - ssd-2
  Coordinators           - 1
  Usable Regions         - 1

<redacted>
```

## Why this project doesn't use elfo?

[Elfo](https://github.com/elfo-rs/elfo) is an actor system in Rust I worked on. It is designed to handle massively concurrent workloads with lots of I/O, which seems like exactly the thing Rhino does.

I actually made an attempt to switch to elfo at one point. There are a couple of reasons on why this wasn't committed:

1. **Elfo actor addresses are not unique across actor restarts.** Imagine one actor has address X. Then this actor dies and another one is spawned. It's possible for this new actor to have address X. Old actor and new actor could belong to different tenants, meaning that if business logic uses actor addresses to send messages, you can send one tenant's information to another tenant, which is a data breach. Not using actor addresses becomes tedious very fast because each and every message needs to have a routing id to make sure it goes to the actor it was intended for. The fact that elfo messages don't support generics forces one to invent a mess of traits to ensure a unified way to handle these routing ids.
2. **Elfo is extremely dynamic.** Actors die and restart all the time and it can be hard to understand what's going on at times. Rhino intends to be a relatively simple service, which does not need most of that flexibility.
