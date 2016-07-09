package org.kuttz.http;

import com.polimi.crapp.ApplicationMaster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Path("api/")
public class Controller {

  public volatile static AMRMClient<AMRMClient.ContainerRequest> rmClient;
  public volatile static NMClient nmClient;
  public volatile static org.apache.hadoop.fs.Path resourcePath;
  public volatile static Configuration conf;

  private static Map<String, Container> containerMap =
      new HashMap<String, Container>();

  @GET
  @Path("stop/{cId}")
  @Produces(MediaType.TEXT_PLAIN)
  public String stop(@PathParam("cId") String cId) {
    System.err.println("RECVD: Stop Container [" + cId + "]");
    ContainerId containerId = ContainerId.fromString(cId);
    try {
      nmClient.stopContainer(containerId,
          containerMap.get(containerId.toString()).getNodeId());
      return "DONE";
    } catch (YarnException e) {
      return "Got YarnException from NM [" + e.getMessage() + "]";
    } catch (IOException e) {
      return "Got IOException from NM [" + e.getMessage() + "]";
    }
  }

  @GET
  @Path("status/{cId}")
  @Produces(MediaType.TEXT_PLAIN)
  public String status(@PathParam("cId") String cId) {
    StringBuilder resp = new StringBuilder();
    log(resp, "RECVD: Container status [" + cId + "]");
    ContainerId containerId = ContainerId.fromString(cId);
    log(resp, "Input ContainerId fromString [" + containerId + "]");
    Container container = containerMap.get(containerId.toString());
    log(resp, "Cached value [" + container + "]");

    log(resp, "Are container Ids equal ? [" + (container.getId() == containerId) + "]");

    if (container.getId() != containerId) {
      log(resp, "C: [" + container.getId().getApplicationAttemptId() +"]");
      log(resp, "W: [" + containerId.getApplicationAttemptId() + "]");
      log(resp, "C: [" + container.getId().getContainerId() + "]");
      log(resp, "W: [" + containerId.getContainerId() + "]");
    }


    if (container == null) {
      log(resp, "No container in cache");
      return resp.toString();
    }
    try {
      ContainerStatus containerStatus = nmClient.getContainerStatus(
          containerId, container.getNodeId());
      return containerStatus.toString();
    } catch (YarnException e) {
      return "Got YarnException from NM [" + e.getMessage() + "]";
    } catch (IOException e) {
      return "Got IOException from NM [" + e.getMessage() + "]";
    }
  }

  @GET
  @Path("ask/{pri}/{execType}/{mem}/{cores}")
  @Produces(MediaType.TEXT_PLAIN)
  public String ask(@PathParam("pri") int pri,
      @PathParam("execType") String execType,
      @PathParam("mem") int mem,
      @PathParam("cores") int cores) {

    StringBuilder resp = new StringBuilder();
    log(resp, "RECVD: Container ask" +
        "[" + execType + ", " + mem + ", " + cores + "]");
    Container container = null;
    try {
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(pri);
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(mem);
      capability.setVirtualCores(cores);

      ContainerLaunchContext clc =
          Records.newRecord(ContainerLaunchContext.class);

      LocalResource localResource = Records.newRecord(LocalResource.class);
      FileStatus fileStat = null;
      fileStat = FileSystem.get(conf).getFileStatus(resourcePath);
      localResource.setResource(ConverterUtils.getYarnUrlFromPath
          (resourcePath));
      localResource.setSize(fileStat.getLen());
      localResource.setTimestamp(fileStat.getModificationTime());
      localResource.setType(LocalResourceType.FILE);
      localResource.setVisibility(LocalResourceVisibility.PRIVATE);

      clc.setLocalResources(
          Collections.singletonMap("script0.sh", localResource));

      clc.setCommands(
          Collections.singletonList(
              "./script0.sh" +
                  " 1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                  "/stdout" +
                  " 2>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                  "/stderr"
          ));

      // Make container request for container to ResourceManager
      AMRMClient.ContainerRequest containerAsk =
          new AMRMClient.ContainerRequest(capability, null, null, priority,
              true, null, ExecutionTypeRequest.newInstance(
              ((execType.toLowerCase().startsWith("o") ?
                  ExecutionType.OPPORTUNISTIC : ExecutionType.GUARANTEED)),
              true)
          );
      log(resp, "Making res-req:");
      AllocateResponse response = null;
      try {
        ApplicationMaster.allocLock.lock();
        rmClient.addContainerRequest(containerAsk);
        int attempts = 20;
        do {
          log(resp, "BEFORE: Trying [" + attempts + "]");
          response = rmClient.allocate(ApplicationMaster.responseId++);
          log(resp, "AFTER: Trying [" + attempts + "]");
          attempts--;
          Thread.sleep(200);
        } while (attempts > 0 && response.getAllocatedContainers().size() == 0);

        if ((attempts == 0) && response.getAllocatedContainers().size() == 0) {
          log(resp, "Tried 20 times to start container.. not happening !!");
          return resp.toString();
        }
      } finally {
        rmClient.removeContainerRequest(containerAsk);
        ApplicationMaster.allocLock.unlock();
      }

      container = response.getAllocatedContainers().get(0);
      containerMap.put(container.getId().toString(), container);

      log(resp, "BEFORE: Initializing and starting container [" +
          container.getId() + "]");
      nmClient.startContainer(container, clc);
      log(resp, "AFTER: Initializing and starting container [" +
          container.getId() + "]");
      log(resp, "RESP: Container [" + container.getId()
          + ", " + container.getNodeId().getHost() + ":"
          + container.getNodeId().getPort() + "]");
    } catch (Exception e) {
      log(resp, e);
    }
    return resp.toString();
  }

  @GET
  @Path("promote/{cId}")
  @Produces(MediaType.TEXT_PLAIN)
  public String promote(@PathParam("cId") String cId) {
    StringBuilder resp = new StringBuilder();
    log(resp, "RECVD: Promote container [" + cId + "]");
    ContainerId containerId = ContainerId.fromString(cId);
    Container container = null;
    AllocateResponse response = null;
    try {
      ApplicationMaster.allocLock.lock();
      rmClient.updateExecutionType(containerMap.get(containerId.toString()),
          ExecutionType.GUARANTEED);

      int attempts = 20;
      do {
        log(resp, "BEFORE: Trying [" + attempts + "]");
        response = rmClient.allocate(ApplicationMaster.responseId++);
        log(resp, "AFTER: Trying [" + attempts + "]");
        attempts--;
        Thread.sleep(200);
      } while (attempts > 0 && response.getPromotedContainers().size() == 0);

      if ((attempts == 0) && response.getPromotedContainers().size() == 0) {
        log(resp, "Tried 20 times to start container.. not happening !!");
        return resp.toString();
      }

      container = response.getPromotedContainers().get(0);
      containerMap.put(container.getId().toString(), container);

    } catch (Exception e) {
      log(resp, e);
    } finally {
      ApplicationMaster.allocLock.unlock();
    }
    log(resp, "Update cid [" + cId + "] to" +
        "[" + container.getExecutionType() + "]");
    return resp.toString();
  }

  private static void log(StringBuilder resp, String str) {
    System.out.println(str);
    resp.append(str + "\n");
  }
  private static void log(StringBuilder resp, Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    System.out.println("Got Exception.... ---->\n");
    System.out.println(sw.toString());
    resp.append("Got Exception.... ---->\n");
    resp.append(sw.toString());
  }
}
