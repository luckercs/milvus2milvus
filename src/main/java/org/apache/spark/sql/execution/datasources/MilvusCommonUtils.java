package org.apache.spark.sql.execution.datasources;

import io.milvus.common.utils.Float16Utils;

import java.util.*;
import java.nio.ByteBuffer;

public class MilvusCommonUtils {
    public static List<Float> generateFloatVector(int dimension) {
        Random ran = new Random();
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; ++i) {
            vector.add(ran.nextFloat());
        }
        return vector;
    }

    public static List<Float> generateFloatVector(int dimension, Float initValue) {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimension; ++i) {
            vector.add(initValue);
        }
        return vector;
    }

    public static List<List<Float>> generateFloatVectors(int dimension, int count) {
        List<List<Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            List<Float> vector = generateFloatVector(dimension);
            vectors.add(vector);
        }
        return vectors;
    }

    public static ByteBuffer generateBinaryVector(int dimension) {
        Random ran = new Random();
        int byteCount = dimension / 8;
        // binary vector doesn't care endian since each byte is independent
        ByteBuffer vector = ByteBuffer.allocate(byteCount);
        for (int i = 0; i < byteCount; ++i) {
            vector.put((byte) ran.nextInt(Byte.MAX_VALUE));
        }
        return vector;
    }

    public static List<ByteBuffer> generateBinaryVectors(int dimension, int count) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            ByteBuffer vector = generateBinaryVector(dimension);
            vectors.add(vector);
        }
        return vectors;
    }

    public static SortedMap<Long, Float> generateSparseVector() {
        Random ran = new Random();
        SortedMap<Long, Float> sparse = new TreeMap<>();
        int dim = ran.nextInt(10) + 10;
        for (int i = 0; i < dim; ++i) {
            sparse.put((long) ran.nextInt(1000000), ran.nextFloat());
        }
        return sparse;
    }

    public static List<SortedMap<Long, Float>> generateSparseVectors(int count) {
        List<SortedMap<Long, Float>> vectors = new ArrayList<>();
        for (int n = 0; n < count; ++n) {
            SortedMap<Long, Float> sparse = generateSparseVector();
            vectors.add(sparse);
        }
        return vectors;
    }

    public static ByteBuffer generateFloat16Vector(int dimension, boolean bfloat16) {
        List<Float> originalVector = generateFloatVector(dimension);
        return encodeFloat16Vector(originalVector, bfloat16);
    }

    public static List<ByteBuffer> generateFloat16Vectors(int dimension, int count, boolean bfloat16) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ByteBuffer buf = generateFloat16Vector(dimension, bfloat16);
            vectors.add((buf));
        }
        return vectors;
    }

    public static ByteBuffer encodeFloat16Vector(List<Float> originVector, boolean bfloat16) {
        if (bfloat16) {
            return Float16Utils.f32VectorToBf16Buffer(originVector);
        } else {
            return Float16Utils.f32VectorToFp16Buffer(originVector);
        }
    }

    public static List<Float> decodeFloat16Vector(ByteBuffer buf, boolean bfloat16) {
        if (bfloat16) {
            return Float16Utils.bf16BufferToVector(buf);
        } else {
            return Float16Utils.fp16BufferToVector(buf);
        }
    }

    public static List<ByteBuffer> encodeFloat16Vectors(List<List<Float>> originVectors, boolean bfloat16) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (List<Float> originVector : originVectors) {
            if (bfloat16) {
                vectors.add(Float16Utils.f32VectorToBf16Buffer(originVector));
            } else {
                vectors.add(Float16Utils.f32VectorToFp16Buffer(originVector));
            }
        }
        return vectors;
    }
}
