package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {
	 
	 private double[] vector;
	 
	 public Vector() {
	  super();
	 }
	 
	 public Vector(Vector v) {
	  super();
	  int l = v.vector.length;
	  this.vector = new double[l];
	  System.arraycopy(v.vector, 0, this.vector, 0, l);
	 }
	 
	 public Vector(double x, double y) {
	  super();
	  this.vector = new double[] { x, y };
	 }
	 
	 @Override
	 public void write(DataOutput out) throws IOException {
	  out.writeInt(vector.length);
	  for (int i = 0; i < vector.length; i++)
	   out.writeDouble(vector[i]);
	 }
	 
	 @Override
	 public void readFields(DataInput in) throws IOException {
	  int size = in.readInt();
	  vector = new double[size];
	  for (int i = 0; i < size; i++)
	   vector[i] = in.readDouble();
	 }
	 
	 @Override
	 public int compareTo(Vector o) {
	 
//	  boolean equals = true;
	  for (int i = 0; i < vector.length; i++) {
	     int c = (int) (vector[i] - o.vector[i]);
	     if (c != 0.0d) {
	     return c;
	     }
	  }
	  return 0;
	 }
	   // get and set omitted
	 
	}
