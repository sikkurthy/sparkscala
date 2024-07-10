package com.learn.spark

object ClimStairs extends App{

  println(climb(5));
  def climb(num:Int): Int = {

    if (num==1) {
      return 1
    }
    if(num==2){
      return 2
    }
    val oneStep=climb(num-1)
    val twoStep=climb(num-2)
    return oneStep+twoStep
  }
}
