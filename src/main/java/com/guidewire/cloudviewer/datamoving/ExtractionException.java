package com.guidewire.cloudviewer.datamoving;

/**
 * Class description...
 */
public class ExtractionException {
  private String toString;
  private String message;
  private String stackTrace;

  public ExtractionException(String msg, Exception e) {
    toString = e.toString();
    message = msg + e.getMessage();
    stackTrace = getStackTrace(e);
  }

  public String getToString() {
    return toString;
  }

  public String getMessage() {
    return message;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  /**
   * Creates a stack trace akin to what is sent to logs
   * However, it focuses on describing where in the guidewire code things went amiss, ignoring
   * what is happening in the rest of the stack.
   * @param e
   * @return a stack trace
   */
  private String getStackTrace(Exception e) {
    StringBuffer buffer = new StringBuffer();
    for (StackTraceElement stackTraceElement : e.getStackTrace()) {
      String candidateString = createStackTraceString(stackTraceElement);
      buffer.append(candidateString);
    }
//    System.out.println("buffer: " + buffer);
//    e.printStackTrace();
//    boolean isInGuidewireClasses = true;
//    for (StackTraceElement stackTraceElement : e.getStackTrace()) {
//      String candidateString = createStackTraceString(stackTraceElement);
//      boolean describesGuidewireClass = candidateString.contains("guidewire");
//      if (isInGuidewireClasses || describesGuidewireClass) {
//        buffer.append(candidateString);
//      }
//      isInGuidewireClasses = describesGuidewireClass;
//    }
    return buffer.toString();
  }

  private String createStackTraceString(StackTraceElement stackTraceElement) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(stackTraceElement.getClassName()).append(" ");
    buffer.append(stackTraceElement.getMethodName()).append(" ");
    buffer.append(stackTraceElement.getLineNumber()).append("\n");
    return buffer.toString();
  }
}
