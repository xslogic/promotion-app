package com.polimi.crapp;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.kuttz.http.Controller;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ApplicationMaster {

  public static Lock allocLock = new ReentrantLock();
  public static int responseId = 0;

  public static void main(String[] args) throws Exception {

    Controller.resourcePath = new Path(args[0]);

    // Initialize clients to ResourceManager and NodeManagers
    Controller.conf = new YarnConfiguration();
    Controller.conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS);
    Controller.rmClient = AMRMClient.createAMRMClient();
    Controller.rmClient.init(Controller.conf);
    Controller.rmClient.start();

    Controller.nmClient = NMClient.createNMClient();
    Controller.nmClient.init(Controller.conf);
    Controller.nmClient.start();

    // Register with ResourceManager
    System.out.println("BEFORE: registerApplicationMaster");
    Controller.rmClient.registerApplicationMaster("", 0, "");
    System.out.println("AFTER: registerApplicationMaster");

    System.out.println("BEFORE: Controller start");
    HttpServer httpServer = HTTPServerFactory.create("org.kuttz.http");
    httpServer.start();
    System.out.println("AFTER: Controller started on port ["
        + httpServer.getAddress().getPort() + "]");


    while (true) {
      try {
        allocLock.lock();
        Controller.rmClient.allocate(responseId++);
      } finally {
        allocLock.unlock();
      }
      Thread.sleep(1000);
    }
  }
}