package com.guidewire.cloudviewer.datamoving;

import java.util.List;

/**
 * This class is passed from the server to the client, and contains a list of commands that we should run
 * on the client to get diagnostic information.
 */
public class TestToolCommands {
  private List<String[]> commands = null;
  Boolean isWindows = null;

  public TestToolCommands(List<String[]> cmds, Boolean isWndows) {
    commands = cmds;
    isWindows = isWndows;
  }

  public List<String[]> getCommands() {
    return commands;
  }

  public void setCommands(List<String[]> cmds) {
    commands = cmds;
  }

  public boolean isWindows() {
    return isWindows;
  }

//  public static TestToolCommandsDeserializer createJSONDeserializer() {
//    return new TestToolCommandsDeserializer();
//  }
//
//  public static class TestToolCommandsDeserializer implements JsonDeserializer<TestToolCommands> {
//    @Override
//    public TestToolCommands deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
//      try {
//        JsonObject jsonObject = (JsonObject)json;
//        String type = jsonObject.get("type").getAsString();
//        String name = jsonObject.get("name").getAsString();
//        String formatting = null;
//        if (jsonObject.get("formatString") != null) {
//          formatting = ((JsonObject) json).get("formatString").getAsString();
//        }
//        return TestToolCommands.createDefinition(type, name, formatting);
//      } catch (Throwable t) {
//        t.printStackTrace();
//        return null;
//      }
//    }
//  }
}
