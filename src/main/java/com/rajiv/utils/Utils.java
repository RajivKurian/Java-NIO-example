package com.rajiv.utils;

public class Utils {
  public static final int nextPowerOfTwo(int num) {
    return 1 << (32 - Integer.numberOfLeadingZeros(num - 1));
  }


  public static int log2(int n) {
    if(n <= 0) return -1;
    return 31 - Integer.numberOfLeadingZeros(n);
}
}