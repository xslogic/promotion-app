package com.polimi.crapp;

import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.net.httpserver.HttpServer;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.UnknownHostException;

public class HTTPServerFactory {

  public static HttpServer create(String resourcePackage) throws
      IOException {
    PackagesResourceConfig resConfig = new PackagesResourceConfig(
        resourcePackage);
    return HttpServerFactory.create(getURI(findFreePort()), resConfig);
  }

  private static URI getURI(int port) {
    return UriBuilder.fromUri("http://" + getHostname() + "/")
        .port(port).build();
  }

  private static String getHostname() {
    String hostName = "0.0.0.0";
    try {
      hostName = InetAddress.getByAddress(new byte[] {0, 0, 0, 0})
          .getCanonicalHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return hostName;
  }

  private static int findFreePort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } catch (IOException e) {
      // Ignore
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }
    throw new IOException("Could not find a free TCP/IP port");
  }

  public static void main(String[] args) throws IOException {
    HttpServer httpServer = HTTPServerFactory.create("org.kuttz.http");
    httpServer.start();
    System.out.println("\nRunning on port : " + httpServer.getAddress().getPort());
  }
}
