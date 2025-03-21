---
title: MCP Server
rank: 5
---

# MCP Server

This guide provides tips for developing the MCP (Model Context Protocol) server for Sail.

### Running the MCP Server

Run the following command to build the Sail CLI and install it in the `default` Hatch environment.

```bash
hatch run maturin develop
```

Run `hatch shell` to enter the `default` Hatch environment.
Then use the following command to launch the MCP server.

```bash
sail spark mcp-server
```

The steps above can be inconvenient if you are only making changes to the MCP server Python script.
In these steps, the Python script is embedded in the Sail binary. Changing the Python script requires rebuilding the binary for the change to take effect, but building the binary is known to be slow.

To work around this, you can run the Python script directly, using the following command in the `default` Hatch environment.

```bash
python crates/sail-cli/src/spark/mcp_server.py
```

## Using the MCP Inspector

The MCP project offers an inspector for MCP server development. To run the inspector, use the following command.

```bash
pnpx @modelcontextprotocol/inspector
```

The inspector will show its local URL in the terminal.

Open the URL in the browser to use the inspector.
Enter the URL of the MCP server in the left panel and click on the **Connect** button.
For example, if your MCP server is running locally on port `8000` using the SSE (Server-Sent Events) transport, the URL should be `http://127.0.0.1:8000/sse`.

::: info
Using `localhost` in the MCP server URL may not work.
Depending on your network configuration, `localhost` may resolve to an IPv6 address, while the MCP server may be listening on an IPv4 address.
:::

You can now use the inspector to send and receive messages to and from the MCP server.

For more information, please refer to the [MCP documentation](https://modelcontextprotocol.io/).
