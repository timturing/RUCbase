## NOTES:

我是新开的两个分支，分别为lab2开出来的bpluscon和lab3开出来的orderby，切换分支检查时请记得remake，在orderby分支下有提供./task_orderby.sh直接测试，修改input_orderby.sql的内容即可。

(如果有需要最后再merge到主分支上)

## B plus tree Concurrency

### Basic Latch Crabbing Protocol

B+ tree 节点的获取锁顺序遵循从上至下的原则，如果两个线程获取锁的顺序相反则会出现死锁情况。所以在树修改过程中不允许出现从下向上获取父节点锁的行为，这就要求在获取锁的过程中要一次性获取所有可能出现改动 (split/coalsece/redistribte/修改内容) 的 node。通过检查 node 元素数量 `n` 可以分辨出该 node 是否 safe(不会发生 split/coalsece/redistribte) 即 `n < max_size - 1 && n > max_size / 2`。crabbing 的具体实现是，从 root node 开始向 leaf node 遍历，获取每一个 node 的写锁之后将其插入一个双端队列中。如果判断当前的 node 是一个 safe node，就将队列中该 node 之前所有的 node 解锁并弹出。最终队列中会剩下包括 leaf node 在内所有可能出现改动的 node。在 split 的过程中由于新分配的 node 只有当前线程可见所以无需加锁，而在实现 coalsece/redistribute 的过程中则必须需要获取选中的 sbiling node 的锁，因为此时可能出现竞争的情况。

### Root Latching

由于 B+ tree 还有可能出现 root node 的变化，这里需要特殊的处理。思路是额外设置一个 root_latch 在 crabbing 的过程中每次解锁 node 时检查是否满足释放 root_latch 的条件，即 root node 不会出现改动，如果满足条件即可释放 root_latch。

### Improved Lock Crabbing Protocol

由于 page_size 的关系，节点发生 split/coalsece/redistribte 的概率是非常低的，所以可以实现一个乐观优化。每次改动从 root node 开始像 search 过程中那样获取读锁，只有到了 leaf node 再获取写锁，如果此时双端队列中不止有 leaf node 这一个 node，说明需要 split/coalsece/redistribte。这时将整个队列中的 node 都释放掉，从 root node 开始重新顺序获取写锁。

### Notes

本来是按着要求来的，但是就是过不了，原因在于我先FindleafPage了，然后根据是否有安全节点返回一个bool，然后再决定是否加root_latch，也就是说执行逻辑是**先FindleafPage再决定是否加锁**。

然而正确的做法是，对于insert类型，你得先控制住root，否则一个线程在FindleafPage，另一个也在FindleafPage，即使你两个本来都要返回需要修改root也为时已晚，已经被更新了。所以正确顺序：**先锁root，根据是否有安全节点决定释放root的时机（是直接能放还是得一直到insert结束）**。

## Order By

涉及的模块包括：parser，

### Parser

首先是语法树，由于SQL语句解析本身就是编译过程，所以我们需要对lex和yacc文件修改

#### lex

需要增加ORDER，BY，ASC，DESC，LIMIT五个词法并返回对应的token给yacc即可。

#### yacc

参照编译原理课上的写法，以及已有的Col的语法规则，我设了一个新的OrderCol，表示用来排序的列：

```
ordercol:
        colName
    {
        $$ = std::make_shared<OrderCol>($1, true);
    }
    |   colName ASC
    {
        $$ = std::make_shared<OrderCol>($1, true);
    }
    |   colName DESC
    {
        $$ = std::make_shared<OrderCol>($1, false);
    }
    ;
orderbyList:
        ordercol
    {
        $$ = std::vector<std::shared_ptr<OrderCol>>{$1};
    }
    |   orderbyList ',' ordercol
    {
        $$.push_back($3);
    }
    ;
```

同样的，由于排序是出现在select类语句中，而已有的select给出的语法规则不具有可拓展性，所以我们直接加上新的语法规则：

```
	/* order by */
    |  SELECT selector FROM tableList optWhereClause ORDER BY orderbyList
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $8, -1);
    }
    /* limit */
    |  SELECT selector FROM tableList optWhereClause LIMIT VALUE_INT
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, std::vector<std::shared_ptr<OrderCol>>{}, $7);
    }
    /* order by and limit */
    |  SELECT selector FROM tableList optWhereClause ORDER BY orderbyList LIMIT VALUE_INT
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $8, $10);
    }
    ;
```

#### Ast.h

上面所说的不具有**可拓展性**实际是因为在SelectStmt结构体中没有其他的成员变量，所以我们得新加，所以上述这种增加语法规则的写法肯定是最好的，同时，为了方便，我也新加了几个构造函数，也就是方便语法规则这里的构造调用。

```cpp
struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>> cols;
    std::vector<std::string> tabs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<OrderCol>> order_cols;
    int limit = -1;
    SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
               std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)) {}
    SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
               std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::vector<std::shared_ptr<OrderCol>> order_cols_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)), order_cols(std::move(order_cols_)){}
    SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
               std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::vector<std::shared_ptr<OrderCol>> order_cols_,
               int limit_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)), order_cols(std::move(order_cols_)), limit(limit_) {}
};
```

至此，底层的语法解析基本就是做完了，但是现在也没法测试，只能全部写完再测试。

### Execution

#### 对接底层内容

追踪上层测试文件的调用路径，发现是从```interp_sql```函数进入的，其中有一部分就是select的解析，负责转换成上层c++的一些结构体，(p.s.其实这种开发感觉挺好，上层的写上层，底层的写底层，中间的接口问题可以事后再单独match)，最终调用了QlManager中的```select_from```函数。所以我们需要新增一个```select_from_orderby```函数。(理论上也可以直接重载函数名，但是无论哪种情况都会导致原来的那种函数调用无法完成其功能，所以无所谓了)。新传入的参数即为语法树中解析到SelectStmt结构体的部分。

```cpp
//修改前
    void select_from(std::vector<TabCol> sel_cols, const std::vector<std::string> &tab_names,std::vector<Condition> conds, Context *context);
//修改后
    void select_from_orderby(std::vector<TabCol> sel_cols, const std::vector<std::string> &tab_names,std::vector<Condition> conds, std::vector<OrderByCol> order_cols, int limit, Context *context);
```

按照上述所说的上层写上层的逻辑，我也新建了一个结构体：

```cpp
//与TabCol结构体对应
struct OrderByCol{
    std::string tab_name;
    std::string col_name;
    bool is_asc;
};
```

然后就到了正式修改执行程序的部分。

#### 修改执行程序

**理论上来说最优的办法当然是再写一个算子，或者修改原有的select算子**，这样肯定是最好的，但是实在是太麻烦了，所以我直接利用已有的select算子，在内存里做排序了。

**在原有的select_from执行程序中，会通过解析的select目标列，在proj算子中直接copy出数据库中的内容**，也就是说如果不新增算子，那么我们只能拿到proj之后的结果，那么我们无法解决**排序列并不是目标列**的情况，根据搜索引擎的反馈，这确实是允许的：

> MySQL中用order by排序的字段不一定要在select语句中

那么如何解决这个问题呢，实际上，虽然我定义了新的OrderCol类型，但是他们仍然是列，所以可以把这些列也丢到投影算子里面，让这些列全都select出来，在内存里排序之后再删除那些不被选择的列。

思路是有了，但是写起来全是挺麻烦的。原来的做法是，算子匹配到对应的record后，每次调用Next从而往后一个一个select出来，然后就直接print了，但是我得存下来，所以用了一个```std::vector<OrderUnit>```来存储所有结果，其中每个OrderUnit结构体存储了某条record在目标列和排序列上的结果。

然后在OrderUnit中写**快排比较规则**，这样就可以调用系统的快排，来保证结果相对高效了。

最后，limit就很简单了，当没有limit时将其置为-1，表示没有limit限制，否则limit>0，那么我在输出时限制结果即可。

