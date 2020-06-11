package com.rogerguo.cymo.curve;

import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Date 6/4/20 9:03 PM
 * @Created by rogerguo
 */
public class HilbertCurveTXY implements SpaceFillingCurve {
    private CompactHilbertCurve compactHilbertCurve = new CompactHilbertCurve(new int[] {21, 21, 21});

    private CompactHilbertCurve compactHilbertCurve2D = new CompactHilbertCurve(new int[] {21, 21});

    @Override
    public String getCurveValueString(int x, int y) {
        List<Integer> bitsPerDimension = compactHilbertCurve2D.getSpec().getBitsPerDimension();
        BitVector[] points = new BitVector[bitsPerDimension.size()];
        for (int i = points.length; --i >= 0;) {
            points[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        points[0].copyFrom(x);
        points[1].copyFrom(y);
        BitVector curveValue = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve2D.getSpec().sumBitsPerDimension());
        compactHilbertCurve2D.index(points, 0, curveValue);

        return curveValue.toString();
    }

    @Override
    public String getCurveValueString(int x, int y, int z) {
        List<Integer> bitsPerDimension = compactHilbertCurve.getSpec().getBitsPerDimension();
        BitVector[] points = new BitVector[bitsPerDimension.size()];
        for (int i = points.length; --i >= 0;) {
            points[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        points[0].copyFrom(z);
        points[1].copyFrom(x);
        points[2].copyFrom(y);
        BitVector curveValue = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve.getSpec().sumBitsPerDimension());
        compactHilbertCurve.index(points, 0, curveValue);

        return curveValue.toString();

    }

    @Override
    public long getCurveValue(int x, int y) {
        List<Integer> bitsPerDimension = compactHilbertCurve2D.getSpec().getBitsPerDimension();
        BitVector[] points = new BitVector[bitsPerDimension.size()];
        for (int i = points.length; --i >= 0;) {
            points[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        points[0].copyFrom(x);
        points[1].copyFrom(y);
        BitVector curveValue = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve2D.getSpec().sumBitsPerDimension());
        compactHilbertCurve2D.index(points, 0, curveValue);

        return curveValue.toLong();
    }

    @Override
    public long getCurveValue(int x, int y, int z) {
        List<Integer> bitsPerDimension = compactHilbertCurve.getSpec().getBitsPerDimension();
        BitVector[] points = new BitVector[bitsPerDimension.size()];
        for (int i = points.length; --i >= 0;) {
            points[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        points[0].copyFrom(z);
        points[1].copyFrom(x);
        points[2].copyFrom(y);
        BitVector curveValue = BitVectorFactories.OPTIMAL.apply(compactHilbertCurve.getSpec().sumBitsPerDimension());
        compactHilbertCurve.index(points, 0, curveValue);

        return curveValue.toLong();
    }

    @Override
    public Map<String, Integer> from3DCurveValue(long curveValue) {
        return null;
    }

    @Override
    public Map<String, Integer> from2DCurveValue(long curveValue) {
        return null;
    }
}

