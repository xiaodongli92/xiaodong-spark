package com.xiaodong.easy.spark

import breeze.linalg
import breeze.linalg.{*, Axis, DenseMatrix, DenseVector, accumulate, argmax, diag, max, sum, trace}

/**
  * breeze基本方法
  */
object BreezeBaseMethod {
    def main(args: Array[String]): Unit = {
        matrixZeros()
        vectorZeros()
        vectorOne()
        vectorFill()
        vectorRange()
        vectorEye()
        vectorDig()
        createMatrix()
        createVector()
        vectorTranspose()
        visitVector()
        visitMatrix()
        matrixReshape()
        matrix2Vector()
        matrixTriangular()
        vectorTriangular()
        matrixCalculate()
        vectorCalculate()
        
    }
    
    /**
      * 向量相关操作
      */
    private def vectorCalculate() = {
        val data = DenseVector(1,2,3,4,5,6,7,8,9)
        println("向量点积")
        println(data dot data)
        println("累计和")
        println(accumulate(data))
    }
    
    /**
      * 矩阵相关计算
      */
    private def matrixCalculate() = {
        val data = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12))
        val dataNew = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12))
        val data1 = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12), (9, 10, 11, 12))
        println(data)
        println("元素相加")
        println(data + dataNew)
        println("元素相乘")
        println(data :* dataNew)
        println("元素相除")
        println(data :/ dataNew)
        println("元素比较")
        println(data :< dataNew)
        println("元素相等")
        println(data :== dataNew)
        println("元素追加")
        println(data :+= 1)
        println("元素追乘")
        println(data :* 2)
        println("元素最大值")
        println(max(dataNew))
        println("元素最大值及位置")
        println(argmax(dataNew))
        println("元素求和")
        println(sum(dataNew))
        println("每一列求和")
        println(sum(dataNew(::, *)))
        println(sum(dataNew, Axis._0))
        println("每一行求和")
        println(sum(dataNew(*, ::)))
        println(sum(dataNew, Axis._1))
        println("对角线求和")
        println(trace(data1))
    }
    
    /**
      * 向量操作
      */
    private def vectorTriangular() = {
        val data = DenseVector(1,2,3,4,5,6,7,8,9)
        val dataNew = DenseVector(1,2,3,4,5,6,7,8,9)
        println(data)
        println("1 to 4")
        println(data(1 to 4))
        println("1 to 4 重新赋值")
        println(data(1 to 4) := DenseVector(1,2,3,4))
        println(data)
        println("向量连接")
        println(DenseVector.vertcat(data, dataNew))
    }
    
    /**
      * 矩阵操作
      */
    private def matrixTriangular() = {
        val data = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12))
        val dataNew = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12))
        println(data)
        println("复制下三角")
        println(linalg.lowerTriangular(data))
        println("复制上三角")
        println(linalg.upperTriangular(data))
        println("矩阵复制")
        println(data.copy)
        println("子集赋值")
        println(data(0 to 1, 0 to 1) := 5)
        println(data)
        println("垂直连接矩阵")
        println(DenseMatrix.vertcat(data, dataNew))
        println("横向连接矩阵")
        println(DenseMatrix.horzcat(data, dataNew))
    }
    
    /**
      * 矩阵转为向量
      * 先行后列的方式
      */
    private def matrix2Vector() = {
        val data = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8))
        println(data)
        println(data.toDenseVector)
    }
    
    /**
      * 调整矩形形状
      * 是先行后列的方式
      */
    private def matrixReshape() = {
        val data = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8))
        println(data)
        println(data.reshape(4, 2))
    }
    
    /**
      * 矩阵元素访问
      */
    private def visitMatrix() = {
        val data = DenseMatrix((1, 2, 3, 4), (5, 6, 7, 8))
        println(data)
        println(data(0, 1))
        println(data(1, ::))
        println(data(::, 1))
    }
    
    /**
      * 向量元素访问
      */
    private def visitVector() = {
        val data = DenseVector(1, 2, 3, 4, 5, 6, 7)
        println(data)
        println(data(0 to 1))
        println(data(1 to 1))
        println(data(5 to 0 by -2))
        println(data(0 to -1 by 3))
    }
    
    /**
      * 向量转置
      */
    private def vectorTranspose() = {
        println("向量转置")
        println(DenseVector(1, 2, 3).t)
    }
    
    /**
      * 按照行创建向量
      * 从函数创建向量
      * 从数组创建向量
      * 0到1的随机向量
      */
    private def createVector() = {
        println("按照行创建向量")
        println(DenseVector(1, 2, 3))
        println("从函数创建向量")
        println(DenseVector.tabulate(3){i => i*2})
        println("从数组创建向量")
        println(new DenseVector(Array(1, 2, 3, 4)))
        println("0到1的随机向量")
        println(DenseVector.rand[Double](4))
    }
    
    /**
      * 按照行创建矩阵
      * 从函数创建矩阵
      * 从数组创建矩阵
      */
    private def createMatrix() = {
        println("按照行创建矩阵")
        println(DenseMatrix((1, 2, 3),(1, 2, 3)))
        println("从函数创建矩阵")
        println(DenseMatrix.tabulate(3, 3){(i, j) => i * j})
        println("从数组创建矩阵")
        println(new DenseMatrix(2, 3, Array(1, 2, 3, 4, 5, 6)))
        println("0到1的随机矩阵")
        println(DenseMatrix.rand[Double](2, 3))
    }
    
    /**
      * 对角矩阵
      */
    private def vectorDig() = {
        println("对角矩阵")
        println(diag(DenseVector(1.0, 2.0, 3.0)))
    }
    
    /**
      * 单位矩阵
      */
    private def vectorEye(): Unit = {
        println("单位矩阵")
        println(DenseMatrix.eye[Double](3))
    }
    
    /**
      * 生成随机向量
      */
    private def vectorRange(): Unit = {
        println("生成随机向量")
        println(DenseVector.range(0, 10, 2))
        println(DenseVector.rangeD(0.0, 10.0, 2.0))
    }
    
    /**
      * 按数值填充向量
      */
    private def vectorFill(): Unit = {
        println("按数值填充向量")
        println(DenseVector.fill(3){5.0})
    }
    
    /**
      * 全1向量
      */
    private def vectorOne(): Unit = {
        println("全1向量")
        println(DenseVector.ones[Double](3))
    }
    
    /**
      * 全0向量
      */
    private def vectorZeros(): Unit = {
        println("全0向量")
        println(DenseVector.zeros[Double](3))
    }
    
    /**
      * 全0矩阵
      */
    private def matrixZeros(): Unit = {
        println("全0矩阵")
        println(DenseMatrix.zeros[Double](2, 3))
    }
}
