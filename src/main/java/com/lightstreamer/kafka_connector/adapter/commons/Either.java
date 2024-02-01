/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.commons;

public class Either<Left, Right> {

  private final Left left;

  private final Right right;

  private Either(Left left, Right right) {
    if (left != null & right == null) {
      this.left = left;
      this.right = null;
      return;
    }
    if (left == null && right != null) {
      this.left = null;
      this.right = right;
      return;
    }
    throw new IllegalArgumentException("Only one parameter can be specified");
  }

  public Left getLeft() {
    return left;
  }

  public Right getRight() {
    return right;
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }

  public static <Left, Right> Either<Left, Right> left(Left left) {
    return new Either<>(left, null);
  }

  public static <Left, Right> Either<Left, Right> right(Right right) {
    return new Either<>(null, right);
  }
}
