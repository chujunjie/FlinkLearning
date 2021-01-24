## Scala语法tip

### 基础
1. 惰性赋值
    lazy val sql = "select * from table"

2. 插值表达式
    val info = s"name=${name}, age=${age}"

3. 三引号
    val sql = """xxx"""

4. ==/equals比较值，eq比较引用

5. for表达式
    5.1 中缀表达式
        for (a <- 1 to 10)
        for (a <- 1.to(10))
    5.2 守卫
        for (a <- 1 to 10 if a % 2 == 0) println(a)
    5.3 推导式
        val arr = for(a <- 1 to 10) yield a * 10
        