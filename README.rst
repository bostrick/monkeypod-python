===========================================================================
MonkeyPod Python Client
===========================================================================

Current Status: Alpha

A Minimal python client for interacting with MonkeyPod.

General MonkeyPod Documentation: https://monkeypod.helpscoutdocs.com/category/134-api


**********************************
Using the command line client
**********************************

::

    $ export MONKEYPOD_API="https://[institution].monkeypod.io/api/v2/"
    $ export MONKEYPOD_TOKEN="..."

    $ monkeypod --help 
    Usage: monkepod [OPTIONS] COMMAND [ARGS]...

      MonkeyPod command line client

      Recognized environment variables:
      MONKEYPOD_API="https://[institution].monkeypod.io/api/v2/"
      MONKEYPOD_TOKEN="..."

    Options:
      -a, --api TEXT    URL in form https://[institution].monkeypod.io/api/v2
      -t, --token TEXT  Authorization token
      --help            Show this message and exit.

    Commands:
      entity  Manage Entities

    $ monkeypod entity --help
    Usage: monkepod entity [OPTIONS] COMMAND [ARGS]...

      Manage Entities

    Options:
      --help  Show this message and exit.

    Commands:
      create  Create entity by specifing .yaml formattted file
      delete  Delete an entry by id or matching email
      match   Search for matching entities

    $ monkeypod entity match -e jane.q.smith@example.com
    ...


******************************
General Notes
******************************


Additional notes I've gathered:

   Base URL:  https://[institution].monkeypod.io/api/v2

Authorization:

    Token should be provided as conventional Authorization header Bearer token:

        Authorization: Bearer <token>

Content-Type: application/json works, not sure if it's requried

    Request 
        Content-type: application/json
        Accept: application/json

Entitiies:

    - There does not seem to be a way to "list all"
        - closest is "match" API, not sure if it supports globbing
        - seems to support exact match only?  how does metadata work?

           CGI params:
            id=, email=, name=, metadata=

    - GET: entity IDs are particulary hard to discover, as you can't list 
      or wildcard match (?)... save them when you create them!

    - POST: contrary to the example, and id does not need to be provided
      on create, it will be generated and returned from the servrer

