package com.xiaodong.spark.examples.mllib

import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 关联规则:
  * 事物1中出现了物品甲
  * 事物2中出现了物品乙
  * 事物3中出现了物品甲和乙
  * 设R = {...}是一组物品集
  * W是一组事物集 W中的每个事物T是一组物品
  * 可信度: 设W中支持物品集A的事物中,有c%事物同时也支持物品集B,c%称为关联规则A->B的可信度
  * 支持度: 设W中有s%的事物同时支持物品集A和B,s%称为关联规则A->B的支持度
  * 期望可信度: 设W中有e%的事物支持物品B,e%称为关联规则A->B的期望可信度
  * 作用度:可信度与期望可信度的比值
  *
  * 可信度是对关联规则的准确度衡量,支持度是对关联规则重要性的衡量
  * 支持度越大,关联规则越重要。有些关联规则可信度虽然很高,但是支持度很低,说明关联规则的实用的机会很小,因此也不重要
  *
  * 为了发现有意义的关联规则,需要给出两个阈值:最小支持度和最小可信度
  */
object AssociationRulesExamples {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        conf.setAppName("AssociationRules")
        conf.setMaster("local")
        val context = new SparkContext(conf)
        val freqItemSets = context.parallelize(Seq(
            new FreqItemset(Array("a"), 15L),
            new FreqItemset(Array("b"), 35L),
            new FreqItemset(Array("a", "b"), 12L)
        ))
        val ar = new AssociationRules().setMinConfidence(0.8)
        val result = ar.run(freqItemSets)
        result.collect().foreach{rule =>
            println(rule.antecedent.mkString(",")
            + "=>"
            + rule.consequent.mkString(",")
            + ":"
            +rule.confidence)
        }
    }
}
