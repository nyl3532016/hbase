/**
 * Autogenerated by Thrift Compiler (0.14.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift2.generated;


/**
 * Specify Durability:
 *  - SKIP_WAL means do not write the Mutation to the WAL.
 *  - ASYNC_WAL means write the Mutation to the WAL asynchronously,
 *  - SYNC_WAL means write the Mutation to the WAL synchronously,
 *  - FSYNC_WAL means Write the Mutation to the WAL synchronously and force the entries to disk.
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.14.1)", date = "2021-03-11")
public enum TDurability implements org.apache.thrift.TEnum {
  USE_DEFAULT(0),
  SKIP_WAL(1),
  ASYNC_WAL(2),
  SYNC_WAL(3),
  FSYNC_WAL(4);

  private final int value;

  private TDurability(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static TDurability findByValue(int value) { 
    switch (value) {
      case 0:
        return USE_DEFAULT;
      case 1:
        return SKIP_WAL;
      case 2:
        return ASYNC_WAL;
      case 3:
        return SYNC_WAL;
      case 4:
        return FSYNC_WAL;
      default:
        return null;
    }
  }
}
