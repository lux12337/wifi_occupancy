# Web Server

We want our Python package to be able to
take data from several different sources
(e.g. local files, database, etc). One of these
sources should be a web server.

In this directory, we define a simple web
server to test our library's capacity to
communicate with web servers.

As of now, our library does **not have that capacity**,
so this web server is not currently being used.

## Getting Started

Make sure to install Node.js and yarn.
If you have a Mac, Homebrew should make this
step very easy.

Install dependencies by changing the
working directory into this
directory and applying the command:
```bash
yarn install
```

To run the server, simply run:
```bash
./compile-and-run.sh
```

## Config

There is one config file
```./src/config.json```.

The parameters for said file should be
contained in the ```./src/Config.ts```
as an interface. This will be used in the
code.
