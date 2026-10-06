"""Instagram Direct connector: personal Instagram accounts to the CRM over Kafka.

Each port (bus, store, proxy, instagram) is a package with its adapters inside;
`runtime` is the core that runs CRM's commands and the inbox polls against the ports
only, and `app` wires the adapters in.
"""
