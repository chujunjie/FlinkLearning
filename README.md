## Scala语法tip

### 基础
1. 变量
    1.1 惰性赋值
        lazy val sql = "select * from table"
    1.2 插值表达式
        val info = s"name=${name}, age=${age}"
    1.3 三引号
        val sql = """xxx"""
    1.4 ==/equals比较值，eq比较引用

2. for表达式
    2.1 中缀表达式
        for (a <- 1 to 10)
        for (a <- 1.to(10))
        for (a <- 1 until 11) // until左闭右开
    2.2 守卫
        for (a <- 1 to 10 if a % 2 == 0) println(a)
    2.3 推导式
        val arr = for(a <- 1 to 10) yield a * 10

3. 方法
    3.1 带名参数
        def add(x: Int = 0, y: Int = 0) = x + y
        add(x = 1)
    3.2 变长参数
        def sum(num: Int*) = num.sum
        sum(1, 2, 3, 4, 5)
    3.3 方法转函数
        val func = add _

4. 数组
    4.1 定长数组Array() 
    4.2 变长数组ArrayBuffer()
        ArrayBuffer +=添加元素 -=删除元素 ++=追加数组
    4.3 max/min/sorted/reverse
    
5. 元组 
    5.1 二元组
        val tuple = name -> age
    5.2 获取元素
        val name = tuple._1
   
6. 列表
    6.1 不可变列表
        val list = List(v1, v2, v3...)
        val list = Nil // 空列表
        val list = v1 :: v2 :: Nil
    6.2 可变列表
        val list = ListBuffer()
        list(0)获取元素 +=追加元素 -=删除元素 ++=追加数组 list.toArray转化为数组
    6.3 常用操作
        ++拼接列表 
        take(n)获取前n个元素 
        drop(n)获取后n个元素 
        flatten扁平化
        拉链zip/拉开unzip listA.zip(listB) -> 元组列表
        mkString拼接元素
        union取并集/intersect取交集/distinct去重
        diff取差集 a.diff(b) 取a中b没有的元素