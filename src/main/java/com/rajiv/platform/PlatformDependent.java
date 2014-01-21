/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.rajiv.platform;

import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


final public class PlatformDependent {

    private static final Unsafe UNSAFE;
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    private static final long CLEANER_FIELD_OFFSET;
    private static final long ADDRESS_FIELD_OFFSET;
    private static final long CAPACITY_FIELD_OFFSET;
    private static final Field CLEANER_FIELD;

    /**
     * {@code true} if and only if the platform supports unaligned access.
     *
     * @see <a href="http://en.wikipedia.org/wiki/Segmentation_fault#Bus_error">Wikipedia on segfault</a>
     */
    private static final boolean UNALIGNED;

    static {
        ByteBuffer direct = ByteBuffer.allocateDirect(1);
        Field cleanerField;
        try {
            cleanerField = direct.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            Cleaner cleaner = (Cleaner) cleanerField.get(direct);
            cleaner.clean();
        } catch (Throwable t) {
            cleanerField = null;
        }
        CLEANER_FIELD = cleanerField;
        //logger.debug("java.nio.ByteBuffer.cleaner: {}", cleanerField != null? "available" : "unavailable");

        Field addressField, capacityField;
        try {
            capacityField = Buffer.class.getDeclaredField("capacity");
            capacityField.setAccessible(true);
            addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            if (addressField.getLong(ByteBuffer.allocate(1)) != 0) {
                addressField = null;
            } else {
                direct = ByteBuffer.allocateDirect(1);
                if (addressField.getLong(direct) == 0) {
                    addressField = null;
                }
                Cleaner cleaner = (Cleaner) cleanerField.get(direct);
                cleaner.clean();
            }
        } catch (Throwable t) {
            addressField = null;
            capacityField = null;
        }
        //logger.debug("java.nio.Buffer.address: {}", addressField != null? "available" : "unavailable");

        Unsafe unsafe;
        if (addressField != null && cleanerField != null && capacityField != null) {
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                //logger.debug("sun.misc.Unsafe.theUnsafe: {}", unsafe != null? "available" : "unavailable");

                // Ensure the unsafe supports all necessary methods to work around the mistake in the latest OpenJDK.
                // https://github.com/netty/netty/issues/1061
                // http://www.mail-archive.com/jdk6-dev@openjdk.java.net/msg00698.html
                try {
                    unsafe.getClass().getDeclaredMethod(
                            "copyMemory",
                            new Class[] { Object.class, long.class, Object.class, long.class, long.class });

                    //logger.debug("sun.misc.Unsafe.copyMemory: available");
                } catch (NoSuchMethodError t) {
                    //logger.debug("sun.misc.Unsafe.copyMemory: unavailable");
                    throw t;
                } catch (NoSuchMethodException e) {
                    //logger.debug("sun.misc.Unsafe.copyMemory: unavailable");
                    throw e;
                }
            } catch (Throwable cause) {
                unsafe = null;
            }
        } else {
            // If we cannot access the address of a direct buffer, there's no point of using unsafe.
            // Let's just pretend unsafe is unavailable for overall simplicity.
            unsafe = null;
        }
        UNSAFE = unsafe;

        if (unsafe == null) {
            CLEANER_FIELD_OFFSET = -1;
            ADDRESS_FIELD_OFFSET = -1;
            CAPACITY_FIELD_OFFSET = -1;
            UNALIGNED = false;
        } else {
            ADDRESS_FIELD_OFFSET = objectFieldOffset(addressField);
            CAPACITY_FIELD_OFFSET = objectFieldOffset(capacityField);
            CLEANER_FIELD_OFFSET = objectFieldOffset(cleanerField);

            boolean unaligned;
            try {
                Class<?> bitsClass = Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
                Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
                unalignedMethod.setAccessible(true);
                unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
            } catch (Throwable t) {
                // We at least know x86 and x64 support unaligned access.
                String arch = System.getProperty("os.arch", "");
                //noinspection DynamicRegexReplaceableByCompiledPattern
                unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64)$");
            }

            UNALIGNED = unaligned;
            //logger.debug("java.nio.Bits.unaligned: {}", UNALIGNED);
        }
    }

    static boolean hasUnsafe() {
        return UNSAFE != null;
    }

    static void throwException(Throwable t) {
        UNSAFE.throwException(t);
    }

    static void freeDirectBufferUnsafe(ByteBuffer buffer) {
        Cleaner cleaner;
        try {
            cleaner = (Cleaner) getObject(buffer, CLEANER_FIELD_OFFSET);
            if (cleaner == null) {
                throw new IllegalArgumentException(
                        "attempted to deallocate the buffer which was allocated via JNIEnv->NewDirectByteBuffer()");
            }
            cleaner.clean();
        } catch (Throwable t) {
            // Nothing we can do here.
        }
    }

    static void freeDirectBuffer(ByteBuffer buffer) {
        if (CLEANER_FIELD == null) {
            return;
        }
        try {
            Cleaner cleaner = (Cleaner) CLEANER_FIELD.get(buffer);
            if (cleaner == null) {
                throw new IllegalArgumentException(
                        "attempted to deallocate the buffer which was allocated via JNIEnv->NewDirectByteBuffer()");
            }
            cleaner.clean();
        } catch (Throwable t) {
            // Nothing we can do here.
        }
    }

    public static long directBufferAddress(ByteBuffer buffer) {
        return getLong(buffer, ADDRESS_FIELD_OFFSET);
    }

    public static void setDirectBufferAddress(ByteBuffer buffer, long address) {
      UNSAFE.putLong(buffer, ADDRESS_FIELD_OFFSET, address);
    }

    public static int directBufferCapacity(ByteBuffer buffer) {
      return getInt(buffer, CAPACITY_FIELD_OFFSET);
    }

    public static void setDirectBufferCapacity(ByteBuffer buffer, int capacity) {
      UNSAFE.putInt(buffer, CAPACITY_FIELD_OFFSET, capacity);
    }

    static long arrayBaseOffset() {
        return UNSAFE.arrayBaseOffset(byte[].class);
    }

    static Object getObject(Object object, long fieldOffset) {
        return UNSAFE.getObject(object, fieldOffset);
    }

    static int getInt(Object object, long fieldOffset) {
        return UNSAFE.getInt(object, fieldOffset);
    }

    private static long getLong(Object object, long fieldOffset) {
        return UNSAFE.getLong(object, fieldOffset);
    }

    static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    static short getShort(long address) {
        if (UNALIGNED) {
            return UNSAFE.getShort(address);
        } else if (BIG_ENDIAN) {
            return (short) (getByte(address) << 8 | getByte(address + 1) & 0xff);
        } else {
            return (short) (getByte(address + 1) << 8 | getByte(address) & 0xff);
        }
    }

    static int getInt(long address) {
        if (UNALIGNED) {
            return UNSAFE.getInt(address);
        } else if (BIG_ENDIAN) {
            return getByte(address) << 24 |
                  (getByte(address + 1) & 0xff) << 16 |
                  (getByte(address + 2) & 0xff) <<  8 |
                   getByte(address + 3) & 0xff;
        } else {
            return getByte(address + 3) << 24 |
                  (getByte(address + 2) & 0xff) << 16 |
                  (getByte(address + 1) & 0xff) <<  8 |
                   getByte(address) & 0xff;
        }
    }

    static long getLong(long address) {
        if (UNALIGNED) {
            return UNSAFE.getLong(address);
        } else if (BIG_ENDIAN) {
            return (long) getByte(address) << 56 |
                  ((long) getByte(address + 1) & 0xff) << 48 |
                  ((long) getByte(address + 2) & 0xff) << 40 |
                  ((long) getByte(address + 3) & 0xff) << 32 |
                  ((long) getByte(address + 4) & 0xff) << 24 |
                  ((long) getByte(address + 5) & 0xff) << 16 |
                  ((long) getByte(address + 6) & 0xff) <<  8 |
                   (long) getByte(address + 7) & 0xff;
        } else {
            return (long) getByte(address + 7) << 56 |
                  ((long) getByte(address + 6) & 0xff) << 48 |
                  ((long) getByte(address + 5) & 0xff) << 40 |
                  ((long) getByte(address + 4) & 0xff) << 32 |
                  ((long) getByte(address + 3) & 0xff) << 24 |
                  ((long) getByte(address + 2) & 0xff) << 16 |
                  ((long) getByte(address + 1) & 0xff) <<  8 |
                   (long) getByte(address) & 0xff;
        }
    }

    static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    static void putShort(long address, short value) {
        if (UNALIGNED) {
            UNSAFE.putShort(address, value);
        } else if (BIG_ENDIAN) {
            putByte(address, (byte) (value >>> 8));
            putByte(address + 1, (byte) value);
        } else {
            putByte(address + 1, (byte) (value >>> 8));
            putByte(address, (byte) value);
        }
    }

    static void putInt(long address, int value) {
        if (UNALIGNED) {
            UNSAFE.putInt(address, value);
        } else if (BIG_ENDIAN) {
            putByte(address, (byte) (value >>> 24));
            putByte(address + 1, (byte) (value >>> 16));
            putByte(address + 2, (byte) (value >>> 8));
            putByte(address + 3, (byte) value);
        } else {
            putByte(address + 3, (byte) (value >>> 24));
            putByte(address + 2, (byte) (value >>> 16));
            putByte(address + 1, (byte) (value >>> 8));
            putByte(address, (byte) value);
        }
    }

    static void putLong(long address, long value) {
        if (UNALIGNED) {
            UNSAFE.putLong(address, value);
        } else if (BIG_ENDIAN) {
            putByte(address, (byte) (value >>> 56));
            putByte(address + 1, (byte) (value >>> 48));
            putByte(address + 2, (byte) (value >>> 40));
            putByte(address + 3, (byte) (value >>> 32));
            putByte(address + 4, (byte) (value >>> 24));
            putByte(address + 5, (byte) (value >>> 16));
            putByte(address + 6, (byte) (value >>> 8));
            putByte(address + 7, (byte) value);
        } else {
            putByte(address + 7, (byte) (value >>> 56));
            putByte(address + 6, (byte) (value >>> 48));
            putByte(address + 5, (byte) (value >>> 40));
            putByte(address + 4, (byte) (value >>> 32));
            putByte(address + 3, (byte) (value >>> 24));
            putByte(address + 2, (byte) (value >>> 16));
            putByte(address + 1, (byte) (value >>> 8));
            putByte(address, (byte) value);
        }
    }

    static void copyMemory(long srcAddr, long dstAddr, long length) {
        UNSAFE.copyMemory(srcAddr, dstAddr, length);
    }

    static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
        UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, length);
    }

    private PlatformDependent() {
    }
}