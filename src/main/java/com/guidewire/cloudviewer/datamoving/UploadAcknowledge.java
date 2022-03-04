package com.guidewire.cloudviewer.datamoving;

/**
* Class description...
*/
public class UploadAcknowledge {
  private String message;
  private int rowsUploaded;
  private boolean success;

  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }

  public int getRowsUploaded() {
    return rowsUploaded;
  }
  public void setRowsUploaded(int rowsUploaded) {
    this.rowsUploaded = rowsUploaded;
  }

  public boolean isSuccess() {
    return success;
  }
  public void setSuccess(boolean success) {
    this.success = success;
  }
}
