import json

# 此类会被跑分服务器继承， 可以在类中自由添加自己的prompt构建逻辑, 除了parse_table 和 run_inference_llm 两个方法不可改动
# 注意千万不可修改类名和下面已提供的三个函数名称和参数， 这三个函数都会被跑分服务器调用
class submission():
    def __init__(self, table_meta_path):
        self.table_meta_path = table_meta_path

    # 此函数不可改动, 与跑分服务器端逻辑一致， 返回值 grouped_by_db_id 是数据库的元数据（包含所有验证测试集用到的数据库）
    # 请不要对此函数做任何改动
    def parse_table(self, table_meta_path):
        with open(table_meta_path, 'r') as db_meta:
            db_meta_info = json.load(db_meta)
        # 创建一个空字典来存储根据 db_id 分类的数据
        grouped_by_db_id = {}

        # 遍历列表中的每个字典
        for item in db_meta_info:
            # 获取当前字典的 db_id
            db_id = item['db_id']

            # 如果 db_id 已存在于字典中，将当前字典追加到对应的列表
            if db_id in grouped_by_db_id:
                grouped_by_db_id[db_id].append(item)
            # 如果 db_id 不在字典中，为这个 db_id 创建一个新列表
            else:
                grouped_by_db_id[db_id] = [item]
        return grouped_by_db_id

    # 此为选手主要改动的逻辑， 此函数会被跑分服务器调用，用来构造最终输出给模型的prompt， 并对模型回复进行打分。
    # 当前提供了一个最基础的prompt模版， 选手需要对其进行改进
    def construct_prompt(self, current_user_question):
        question_type = current_user_question['question_type']
        user_question = current_user_question['user_question']

        system_prompt_1 = "你是一个数据库专家"
        if question_type == 'text2sql':
            current_db_id = current_user_question['db_id']
            cur_db_info = self.parse_table(self.table_meta_path)[current_db_id]
            count_cn = 0  #判断题目是中文还是英文
            for char in user_question:
                if '\u4e00' <= char <= '\u9fff':
                    count_cn += 1  # 统计汉字的数量
            if count_cn>1:
                question_ch = True ##question_ch是标志，如果为True 则题目为中文
            else:
                question_ch = False

            ##当前题目的 database schema
            ##也是最初始的一种schema
            fields = ""
            for i, name in enumerate(cur_db_info[0]['table_names_original']):
                fields +=  name + '('
                for index, row in cur_db_info[0]['column_names_original']:
                    if index == i:
                        fields += row + ','
                    if index ==i+1 and index!=-1:
                        fields = fields[:-1]
                        fields += ");"
                        break
            fields = fields[:-1]
            fields += ");"

            ##这里的目的是为了构建 指定格式的prompt中的database schema
            ##fields_1 是最终得到的用于user_prompt_1的

            # 使用分号分割字符串
            segments_talble = fields.split(';')
            # 移除最后一个空字符串（如果存在）
            if segments_talble[-1] == '':
                segments_talble = segments_talble[:-1]
            fields_1 = ""
            #print('本题数据库\n')
            for segment_table in segments_talble:

                #print(segment_table)
                fields_1 +="# " + str(segment_table) + ";\n"

            fields_1 = fields_1.rstrip('\n')

            ##这里的目的是为了构建 指定格式的外键
            ##foreign_keys_1 是最终得到的用于user_prompt_1的
            ##当前题目的外键
            foreign_keys = ""
            for foreign_key in cur_db_info[0]['foreign_keys']:
                first, second = foreign_key
                first_index, first_column = cur_db_info[0]['column_names_original'][first]
                second_index, second_column = cur_db_info[0]['column_names_original'][second]
                foreign_keys += cur_db_info[0]['table_names_original'][first_index] + '.' + first_column + " = " + \
                                cur_db_info[0]['table_names_original'][second_index] + '.' + second_column + ','
            foreign_keys = foreign_keys[:-1]

            segments_forkeys = foreign_keys.split(",")
            if segments_forkeys[-1] == '':
                segments_forkeys = segments_forkeys[:-1]

            foreign_keys_1 = ""
            #print('本题外键\n')
            for segment_forkeys in segments_forkeys:

                #print(segment_forkeys)
                foreign_keys_1 +="# " + str(segment_forkeys) + ";\n"
            foreign_keys_1 = foreign_keys_1.rstrip('\n')

            ##第一次user_prompt_1
            user_prompt_1 = f'''### Answer the question by SQLite SQL query only and with no explanation.
  You must minimize SQL execution time while ensuring correctness.
### Sqlite SQL tables, with their properties:
#
{fields_1}
#
### Foreign key information of SQLite tables, used for table joins:
#
{foreign_keys_1}
#
### Question:{user_question}
### SQL:
'''         #第一步生成一个初始的schema_link
            ##第一次system_prompt_1
            system_prompt_1 = "You are a database expert."

            ##第一次message
            message_1 = [
                {"role": "system", "content": system_prompt_1},
                {"role": "user", "content": user_prompt_1}
            ]

            ###初步生成的回答，经过处理后，得到初步的presql 为 sql_1
            answer1 = self.run_inference_llm(message_1)
            sql_1 = " ".join(answer1.replace("\n", " ").split())
            if "```sql" in sql_1:
                sql_1 = sql_1.split("```sql")[1].split("```")[0]


            ##提取shcema_links
            Table_use = []
            fields_shc = ""
            #有表被用到了 大小写问题？
            #没有表被用到了(这种情况下说明出bug了) TABLE
            for i, name in enumerate(cur_db_info[0]['table_names_original']):
                if (("FROM "+name).upper() in sql_1.upper()) or (("JOIN "+name).upper() in sql_1.upper()):
                    Table_use.append(name)
                    fields_shc +=  name + '('
                    for index, row in cur_db_info[0]['column_names_original']:
                        if index == i :
                            fields_shc += row + ','
                        if index ==i+1 and index!=-1:
                            fields_shc = fields_shc[:-1]
                            fields_shc += ");"
                            break
                ##排除表是最后的情况

            ## 如果Table_use为空，则证明没有表在生成的SQL中被使用到
            if len(Table_use)==0:
                fields_1_sch  = fields_1
            ## 如果模型正确使用了表，则Table_use 不为空
            else:
                ##针对于最后一张表也被用到的情况，那么最后的逗号时不会被消去的，所以这里做了判别
                ##删掉逗号，给了相应的);
                if fields_shc[-1]==",":
                    fields_shc = fields_shc[:-1]
                    fields_shc += ");"
                # 使用分号分割字符串
                segments_talble_shc = fields_shc.split(';')
                # 移除最后一个空字符串（如果存在）
                if segments_talble_shc[-1] == '':
                    segments_talble_shc = segments_talble_shc[:-1]

                fields_1_sch = ""

                for segment_talble_shc in segments_talble_shc:
                    fields_1_sch +="# " + str(segment_talble_shc) + ";\n"
                fields_1_sch = fields_1_sch.rstrip('\n')

            ##删改后的外键:

            foreign_keys_sch = ""
            for foreign_key in cur_db_info[0]['foreign_keys']:
                first, second = foreign_key
                first_index, first_column = cur_db_info[0]['column_names_original'][first]
                second_index, second_column = cur_db_info[0]['column_names_original'][second]
                if (cur_db_info[0]['table_names_original'][first_index] in Table_use) and (cur_db_info[0]['table_names_original'][second_index] in Table_use):
                    foreign_keys_sch += cur_db_info[0]['table_names_original'][first_index] + '.' + first_column + " = " + \
                                    cur_db_info[0]['table_names_original'][second_index] + '.' + second_column + ','
            foreign_keys_sch = foreign_keys_sch[:-1]

            #最终要用的
            foreign_keys_1_sch = ""
            #判断提取出来的foreign_keys_sch 是否为空，如果不为空，。。如果为空，分解做
            if foreign_keys_sch!="":
                segments_forkeys_sch = foreign_keys_sch.split(",")
                if segments_forkeys_sch[-1] == '':
                    segments_forkeys_sch = segments_forkeys_sch[:-1]
                for segment_forkeys_sch in segments_forkeys_sch:

                    foreign_keys_1_sch +="# " + str(segment_forkeys_sch) + ";\n"
                foreign_keys_1_sch = foreign_keys_1_sch.rstrip('\n')


            ##目前为止我们已经生成了我们想要的table 和 外键 接下来再来一次
            ##如果删除后是否有外键，分不同的user_prompt_2
            if foreign_keys_1_sch=="":
                user_prompt_2 = f'''### Answer the question by SQLite SQL query only and with no explanation.
  You must minimize SQL execution time while ensuring correctness.
### Sqlite SQL tables, with their properties:
#
{fields_1_sch}
#
### Question:{user_question}
### SQL:
'''
            #第一步生成一个初始的schema_link
            else:
                user_prompt_2 = f'''### Answer the question by SQLite SQL query only and with no explanation.
  You must minimize SQL execution time while ensuring correctness.
### Sqlite SQL tables, with their properties:
#
{fields_1_sch}
#
### Foreign key information of SQLite tables, used for table joins:
#
{foreign_keys_1_sch}
#
### Question:{user_question}
### SQL:
'''
            system_prompt_2 = "You are a database expert."
            message_2 = [
                {"role": "system", "content": system_prompt_2},
                {"role": "user", "content": user_prompt_2}
            ]

            #这里是最终生成的sql

            answer_finalsql = self.run_inference_llm(message_2)
            # print(answer_finalsql)
            ## 此处应有自校正模块
            ## 但目前感觉效果不太好，先试试不加的

            ## 最后的规范化SQL命令
            system_prompt_3 = "你是一个格式取消专家"
            user_prompt_3 = f'''### 请删除输入文本首行”```sql“的标志和尾行”```“的标志，并返回纯文本。
### 以下是一些相似输入文本以及输出结果的示例

###示例1
###输入文本:
```sql
SELECT AVG(Num_Employees) 
FROM department 
WHERE Ranking BETWEEN 10 AND 15;
```

### 提取SQL命令:
SELECT AVG(Num_Employees) 
FROM department 
WHERE Ranking BETWEEN 10 AND 15;

###示例2
###输入文本:
SELECT Nationality
FROM journalist
WHERE Years_working > 10
INTERSECT
SELECT Nationality
FROM journalist
WHERE Years_working < 3;

### 提取SQL命令:
SELECT Nationality
FROM journalist
WHERE Years_working > 10
INTERSECT
SELECT Nationality
FROM journalist
WHERE Years_working < 3;

###示例3
###输入文本:
```sql
SELECT country_code
FROM players
GROUP BY country_code
HAVING COUNT(*) > 50;
```

### 提取SQL命令:
SELECT country_code
FROM players
GROUP BY country_code
HAVING COUNT(*) > 50;

### 输入文本：
{answer_finalsql}。

### 提取SQL命令：

'''
            messages = [
                {"role": "system", "content": system_prompt_3},
                {"role": "user", "content": user_prompt_3}
            ]

        elif question_type == 'multiple_choice':
            options = "A." + current_user_question['optionA'] + " B." + current_user_question['optionB'] + " C." + current_user_question[
                'optionC'] + " D." + current_user_question['optionD']
### 第一步根据选择题的类型分为SQL通识类 或者选择整齐而SQL语句类 ，如果出现意外情况则认为时SQL通识类
            system_prompt_type = f'''
### 你是一个SQL数据库专家，擅长分析和分类SQL选择题。你的任务是将提供的SQL选择题分为两类：1. SQL通识类（与SQL概念和操作符相关的基础知识问题），2. 选择正确SQL语句类（要求选择正确的SQL语句来执行某个特定操作）。
### 请根据题目的特征进行分类，并解释你的分类依据。

'''
            #请根据题目的特征进行分类并直接输出题目类型:“通识类题目”或者"选择正确SQL语句类"。
            #分类的主要目的主要时因为2048的token限制导致few shot的提示例子太少了，效果不好
            user_prompt_type = f'''
###请将以下SQL选择题按类型分为通识类和，并解释你的分类依据。

示例1:
### 问题
在SQL中,与“NOT IN”等价的操作符是？

### 选项
A. <> ALL
B. <> SOME
C. = SOME
D. = ALL

### 分类
SQL通识类
### 分类依据
这个问题涉及SQL操作符的基础知识，要求理解各个操作符的意义和用法。

示例2:
### 问题
选择正确的SQL语句来删除表employees中的所有行。

### 选项
A. REMOVE FROM employees;
B. DELETE * FROM employees;
C. TRUNCATE TABLE employees;
D. DROP employees;
### 分类
选择正确SQL语句类

### 分类依据
这个问题要求选择一条正确的SQL语句来执行特定操作。

示例3:
### 问题
有学生表Student(Sno char(8), Sname char(10), Ssex char(2), Sage integer, Dno char(2), Sclass char(6))。要检索学生表中“所有年龄小于等于18岁的学生的年龄及姓名”，SQL语句正确的是？

### 选项
A. Select Sage, Sname From Student ;
B. Select * From Student Where Sage <= 18;
C. Select Sage, Sname From Student Where Sage <= 18;
D. Select Sname From Student Where Sage <= 18;

### 分类
选择正确SQL语句类

### 分类依据
这个问题要求选择一条正确的SQL语句来执行特定操作，即检索学生表中所有年龄小于等于18岁的学生的年龄及姓名。

示例4:
### 问题
在数据库中，哪种类型的键确保表中每行的唯一性？

### 选项
A. Foreign key
B. Primary key
C. Secondary key
D. Composite key

### 分类
SQL通识类

### 分类依据
这个问题涉及数据库中的键类型的基础知识，要求理解每种键的定义和作用。 Primary key用于确保表中每行的唯一性，这属于数据库的基础概念。

示例5:
### 问题
下列（ ）能查询出字段Col的第三个字母是R，但不以W结尾的字符串？

### 选项
A. WHERE Col Like ‘_%R%[^W]’
B. WHERE Col Like ‘__R%[^W]’
C. WHERE Col Like ‘__R%[W]’
D. WHERE Col =‘__R%[^W]’

### 分类
选择正确SQL语句类

### 分类依据
这个问题要求选择一条正确的SQL语句来执行特定操作，即查询字段Col的第三个字母是R，但不以W结尾的字符串。这需要对SQL的LIKE语法及其通配符的用法有深入了解。

示例6:
### 问题
聚合函数中,操作对象可以是元组的函数是？

### 选项
A. AVG
B. COUNT
C. SUM
D. MAX

### 分类
通识类题目

### 分类依据
这个问题涉及SQL聚合函数的基础知识，要求理解各个聚合函数的定义和作用。COUNT函数可以对元组进行操作，这属于数据库的基础概念。

### 现在请对如下选择题分类：

### 问题
{user_question}

### 选项
A. {current_user_question['optionA']}
B. {current_user_question['optionB']}
C. {current_user_question['optionC']}
D. {current_user_question['optionD']}

### 分类:


'''

            message_type = [
                {"role": "system", "content": system_prompt_type},
                {"role": "user", "content": user_prompt_type}
            ]

            assistant_answer_type = self.run_inference_llm(message_type)

            #print('题目类型:{}'.format(assistant_answer_type))

            ###生成 选择题类型知道后续prompt构造

            #类型默认为SQL通识类
            multiple_choice_tpye = "SQL通识类"
            if "选择正确SQL语句类" in assistant_answer_type:
                multiple_choice_tpye = "选择正确SQL语句类"
            elif "SQL通识类" in assistant_answer_type:
                multiple_choice_tpye = "SQL通识类"

            if multiple_choice_tpye == "SQL通识类" :
                system_prompt_1 = '''### 你是一个经验丰富的SQL数据库专家，擅长SQL语法和逻辑推理。
### 在接下来的对话中，你的任务是帮助用户解答与"SQL概念和操作符相关"的基础知识选择题，并通过逐步的逻辑推理过程来选择一个最符合题意的选项。
### 请根据我给出的类似题目的分析过程，按照相同的格式和分析过程给出你的答案。
'''
                user_prompt_1 = f'''请分析以下SQL选择题并选择正确答案。展示你的推理过程。

示例1:

问题: 在SQL中，以下哪个操作符用于判断值在一个指定列表中？
选项：
A. IN
B. NOT IN
C. = ALL
D. <> SOME

回答:
选项A：IN 操作符用于检查某个值是否在指定列表中。例如：SELECT * FROM table WHERE column IN (value1, value2, value3); 返回的是那些列值是 value1, value2 或 value3 的行。因此，IN 是正确答案。

选项B：NOT IN 操作符用于检查某个值是否不在指定列表中，这与问题要求相反。

选项C：= ALL 操作符用于检查某个值是否等于列表中所有值，这不符合问题的要求。

选项D：<> SOME 操作符用于检查某个值是否与列表中的某些值不相等，这也不符合问题的要求。

最符合题意的选项为: A

示例2:

问题: 在SQL中，以下哪个操作符用于判断所有值都不在指定列表中？
选项：
A. <> ALL
B. <> SOME
C. = SOME
D. = ALL

回答:
选项A：<> ALL 操作符用于检查某个值是否与列表中所有值都不相等。例如：SELECT * FROM table WHERE column <> ALL (value1, value2, value3); 相当于 column <> value1 AND column <> value2 AND column <> value3。因此，<> ALL 是正确答案。

选项B：<> SOME 操作符用于检查某个值是否与列表中的某些值不相等，这不符合问题的要求。

选项C：= SOME 操作符用于检查某个值是否等于列表中的某些值，这与问题要求相反。

选项D：= ALL 操作符用于检查某个值是否等于列表中的所有值，这不符合问题的要求。

最符合题意的选项为: A

示例3:

问题:在SQL中，语句ALTER DATABASE属于以下哪类功能？
选项:
A. 数据查询
B. 数据操纵
C. 数据定义
D. 数据控制

回答:
选项A：数据查询是指从数据库中检索数据的操作，通常使用SELECT语句。ALTER DATABASE不是用于数据查询的。因此，这个选项不正确。

选项B：数据操纵是指插入、更新、删除和合并数据的操作，通常使用INSERT、UPDATE、DELETE等语句。ALTER DATABASE不是用于数据操纵的。因此，这个选项不正确。

选项C：数据定义是指定义或修改数据库结构的操作，通常使用CREATE、ALTER、DROP等语句。ALTER DATABASE用于修改数据库的结构或属性，因此属于数据定义操作。这是正确答案。

选项D：数据控制是指控制对数据库访问权限的操作，通常使用GRANT、REVOKE等语句。ALTER DATABASE不是用于数据控制的。因此，这个选项不正确。

最符合题意的选项为: C

示例4: 

问题:在SQL中，哪个函数用于返回指定列中的行数？
选项:
A. SUM
B. COUNT
C. AVG
D. TOTAL

回答:
选项A：SUM函数用于返回数值列的总和，而不是行数。因此，这个选项不正确。

选项B：COUNT函数用于返回指定列中的行数。例如：SELECT COUNT(column_name) FROM table_name; 这个函数计算表中指定列的非NULL值的数量，因此这是正确答案。

选项C：AVG函数用于返回数值列的平均值，而不是行数。因此，这个选项不正确。

选项D：SQL标准中没有TOTAL函数，因此这个选项不正确。

最符合题意的选项为: B

示例5:

问题:在SQL中，哪个语句用于从表中删除一列？
选项
A. DROP COLUMN
B. REMOVE COLUMN
C. DELETE COLUMN
D. ERASE COLUMN

回答:
选项A：DROP COLUMN是标准的SQL语句，用于从表中删除一列。例如：ALTER TABLE table_name DROP COLUMN column_name; 因此，这是正确答案。

选项B：REMOVE COLUMN不是标准的SQL语句，因此这个选项不正确。

选项C：DELETE COLUMN不是标准的SQL语句，因此这个选项不正确。

选项D：ERASE COLUMN不是标准的SQL语句，因此这个选项不正确。

最符合题意的选项为: A

示例6: 

问题:下列哪个不是SQL中的数学函数？
选项:
A. ROUND
B. FLOOR
C. CEIL
D. TRIM

回答:
选项A：ROUND 是一个数学函数，用于将数字四舍五入到指定的小数位数。因此，这个选项是数学函数。

选项B：FLOOR 是一个数学函数，用于返回小于或等于指定数字的最大整数。因此，这个选项是数学函数。

选项C：CEIL 是一个数学函数，用于返回大于或等于指定数字的最小整数。因此，这个选项是数学函数。

选项D：TRIM 不是一个数学函数，而是一个字符串函数，用于去除字符串两端的空格。因此，这个选项不是数学函数。

最符合题意的选项为: D

示例7:

问题:以下哪个不是数据操纵语言DML的操作？

选项:
A. INSERT
B. UPDATE
C. DELETE
D. CREATE

回答:
选项A：INSERT 是数据操纵语言DML的操作，用于向表中插入新行。因此，这个选项是DML操作。

选项B：UPDATE 是数据操纵语言DML的操作，用于更新表中的现有数据。因此，这个选项是DML操作。

选项C：DELETE 是数据操纵语言DML的操作，用于删除表中的现有数据。因此，这个选项是DML操作。

选项D：CREATE 不是数据操纵语言DML的操作，而是数据定义语言DDL的操作，用于创建新的数据库对象（如表、视图、索引等）。因此，这个选项不是DML操作。

最符合题意的选项为:D

示例8:
问题:在SQL中，哪个操作符用于合并两个或多个查询结果集？
选项:
A. UNION
B. JOIN
C. INTERSECT
D. SELECT

回答:
选项A：UNION 操作符用于合并两个或多个查询结果集，并移除重复的记录。例如：SELECT column_name(s) FROM table1 UNION SELECT column_name(s) FROM table2; 因此，UNION 是正确答案。

选项B：JOIN 操作符用于根据相关列将两张表中的记录连接起来，并不用于合并两个或多个查询结果集。因此，这个选项不正确。

选项C：INTERSECT 操作符用于返回两个查询结果集的交集部分，即两个查询结果集中都有的记录。虽然它也可以合并查询结果集的一部分，但不完全符合合并两个或多个查询结果集的定义。因此，这个选项不完全正确。

选项D：SELECT 操作符用于从数据库中选择数据，但不用于合并两个或多个查询结果集。因此，这个选项不正确。

最符合题意的选项为: A

问题：{user_question}
选项：
A. {current_user_question['optionA']}
B. {current_user_question['optionB']}
C. {current_user_question['optionC']}
D. {current_user_question['optionD']}

回答:

'''
            # user_prompt1 = f"我有一个单项选择题，其题目为:{user_question}，选项为：{options}，你的任务如下：1、分析题目，2、对每个选项分析的是否为正确答案并给出理由，3、给出你认为最有可能是正确答案的选项。"
                message_1 = [
                {"role": "system", "content": system_prompt_1},
                {"role": "user", "content": user_prompt_1}
            ]
                assistant_answer_1 = self.run_inference_llm(message_1)
                #print("选择题第一次回答{}".format(assistant_answer_1))

            elif multiple_choice_tpye=="选择正确SQL语句类":
                system_prompt_12 = '''### 你是一个经验丰富的SQL数据库专家，擅长SQL语法和逻辑推理。
### 在接下来的对话中，你的任务是帮助用户解答"要求选择正确的SQL语句来执行某个特定操作"类型的选择题，并通过逐步的逻辑推理过程来选择一个最符合题意的选项。
### 请根据我给出的类似题目的分析过程，按照相同的格式和分析过程给出你的答案。
'''
                user_prompt_12 =f'''请分析以下SQL选择题并选择正确答案。展示你的推理过程。
示例1:
问题:有学生表Student(Sno char(8), Sname char(10), Ssex char(2), Sage integer, Dno char(2), Sclass char(6))。要检索学生表中“所有年龄小于等于18岁的学生的年龄及姓名”，SQL语句正确的是？

选项:
A. Select Sage, Sname From Student;
B. Select * From Student Where Sage <= 18;
C. Select Sage, Sname From Student Where Sage <= 18;
D. Select Sname From Student Where Sage <= 18;

回答:
选项A：这个语句仅仅从Student表中检索Sage和Sname列，但没有添加条件来过滤年龄小于等于18岁的学生。因此，这个选项不符合题目要求。

选项B：这个语句使用了正确的条件过滤了年龄小于等于18岁的学生，但返回了Student表中的所有列，而不是仅仅返回学生的年龄（Sage）和姓名（Sname）。因此，这个选项不完全符合题目要求。

选项C：这个语句不仅使用了正确的条件过滤了年龄小于等于18岁的学生，而且只返回了学生的年龄（Sage）和姓名（Sname）。这个选项完全符合题目要求。

选项D：这个语句使用了正确的条件过滤了年龄小于等于18岁的学生，但仅返回了学生的姓名（Sname），而没有返回年龄（Sage）。因此，这个选项不完全符合题目要求。

最符合题意的选项为: C

示例2:
问题:下面哪个SQL命令用来向表中添加列？

选项:
A. MODIFY TABLE TableName ADD COLUMN ColumnName
B. MODIFY TABLE TableName ADD ColumnName
C. ALTER TABLE TableName ADD COLUMN ColumnName
D. ALTER TABLE TableName ADD ColumnName Type 

回答:
选项A：MODIFY TABLE 不是标准的SQL语法，因此这个选项不正确。

选项B：MODIFY TABLE 不是标准的SQL语法，因此这个选项不正确。

选项C：ALTER TABLE TableName ADD COLUMN ColumnName 这个语法接近正确，但缺少列的数据类型，无法正确添加列。因此，这个选项不完全正确。

选项D：ALTER TABLE TableName ADD ColumnName Type 这个语法是正确的标准SQL语法，用于向表中添加新列，并指定该列的数据类型。例如：ALTER TABLE TableName ADD ColumnName VARCHAR(255); 因此，这个选项是正确答案。

最符合题意的选项为: D

示例3:

问题:数据库中存在学生表S、课程表C和学生选课表SC三个表，它们的结构如下：S(S#，SN，SEX，AGE，DEPT)C(C#，CN)SC(S#，C#，GRADE)其中：S#为学号，SN 为姓名，SEX为性别，AGE为年龄，DEPT为系别，C#为课程号，CN为课程名，GRADE为成绩。请检索选修课程号为C2的学生中成绩最高的学号。( )

选项:
A. SELECT S#，SUM(GRADE)FROM SC WHERE GRADE＞=60 GROUP BY S# ORDER BY 2 DESC HAVING COUNT(*)＞＝4 WHERE C#=“C2” AND GRADE ＞=(SELECT GRADE FORM SC WHERE C#=“C2”)
B. SELECT S# FORM SC WHERE C#=“C2” AND GRADE IN (SELECT GRADE FORM SC WHERE C#=“C2”)
C. SELECT S# FORM SC WHERE C#=“C2” AND GRADE NOT IN (SELECT GRADE FORM SC WHERE C#=“C2”)
D. SELECT S# FORM SC WHERE C#=“C2” AND GRADE＞＝ALL (SELECT GRADE FORM SC WHERE C#=“C2”)

回答:
选项A：这个语句的结构有问题，语法不正确。WHERE条件应放在GROUP BY之前，另外SUM和HAVING部分也不符合题意。因此，这个选项不正确。

选项B：这个语句选择了所有成绩等于子查询中成绩的学生，没有选出最高成绩的学生。因此，这个选项不正确。

选项C：这个语句选择了所有成绩不在子查询中的学生，这与题目要求相反。因此，这个选项不正确。

选项D：这个语句选择了所有成绩大于或等于子查询中所有成绩的学生，使用了ALL关键字，这意味着选择成绩最高的学生。因此，这个选项是正确答案。

最符合题意的选项为: D

示例4:

问题:检索销量表中销量最好的商品id和销量（假设每件商品只有一个订单），下列哪个是SQL语句正确的？

选项
A. SELECT 商品id,销量 FROM 销量表 WHERE 销量=MAX(销量)
B. SELECT 商品id,MAX(销量) FROM 销量表 GROUP BY 销量
C. SELECT 商品id,MAX(销量) FROM 销量表 GROUP BY 商品id
D. SELECT 商品id,销量 FROM 销量表 WHERE 销量=(SELECT MAX(销量) FROM 销量表)

回答:
选项A：SELECT 商品id,销量 FROM 销量表 WHERE 销量=MAX(销量) 这个语句试图直接在WHERE子句中使用聚合函数MAX(销量)，这是不允许的。聚合函数不能直接在WHERE子句中使用。因此，这个选项不正确。

选项B：SELECT 商品id,MAX(销量) FROM 销量表 GROUP BY 销量 这个语句会产生语法错误，因为GROUP BY子句应包含所有非聚合列。这里按销量分组并不是我们需要的结果，因此，这个选项不正确。

选项C：SELECT 商品id,MAX(销量) FROM 销量表 GROUP BY 商品id 这个语句按商品id分组并找到每组中的最大销量，这不符合我们需要的结果，即找到销量最高的商品。因此，这个选项不正确。

选项D：SELECT 商品id,销量 FROM 销量表 WHERE 销量=(SELECT MAX(销量) FROM 销量表) 这个语句首先在子查询中找到最大的销量值，然后在主查询中检索具有该销量值的商品id和销量。这个选项是正确答案。

最符合题意的选项为: D

示例5:

问题:下列（ ）能查询出字段Col的第三个字母是R，但不以W结尾的字符串？

选项:
A. WHERE Col Like ‘_%R%[^W]’
B. WHERE Col Like ‘__R%[^W]’
C. WHERE Col Like ‘__R%[W]’
D. WHERE Col =‘__R%[^W]’

回答:
选项A：WHERE Col Like ‘_%R%[^W]’ 这个模式匹配表示第二个字符是R，并且字符串不能以W结尾。然而，问题要求第三个字母是R，因此这个选项不正确。

选项B：WHERE Col Like ‘__R%[^W]’ 这个模式匹配表示第三个字符是R，并且字符串不能以W结尾。第一个下划线匹配第一个字符，第二个下划线匹配第二个字符，R匹配第三个字符，%表示任意数量的字符，[^W]表示不能以W结尾。这个选项符合问题的要求，因此是正确答案。

选项C：WHERE Col Like ‘__R%[W]’ 这个模式匹配表示第三个字符是R，并且字符串必须以W结尾。这与问题的要求相反，因此这个选项不正确。

选项D：WHERE Col =‘__R%[^W]’ 这个语法是错误的，因为LIKE应当用于模式匹配，而不是等号。因此，这个选项不正确。

最符合题意的选项为: B

问题：{user_question}
选项：
A. {current_user_question['optionA']}
B. {current_user_question['optionB']}
C. {current_user_question['optionC']}
D. {current_user_question['optionD']}

回答：
'''
                message_12 = [
                {"role": "system", "content": system_prompt_12},
                {"role": "user", "content": user_prompt_12}
            ]
                assistant_answer_1 = self.run_inference_llm(message_12)
                #print("选择题(正确SQL语句类)第一次回答{}".format(assistant_answer_1))


            system_prompt_2 = '你是一个经验丰富的SQL数据库专家，请根据选择题的分析提取出正确答案'
            #user_prompt_2 = f'请根据选择题："{user_question}"，选项为：{options}\n的分析是“{assistant_answer_1}”吗？请从更加严谨性，合理性和科学性的角度重新考虑一下这个选择题。。并仅返回选项标号(A/B/C/D)，不要返回其他内容'
            user_prompt_2 = f'''###请根据选择题的分析提取出正确答案并输出，请注意并仅返回正确答案的选项标号(A/B/C/D)，不要返回其他内容。

选择题:
{user_question}
选项：
A. {current_user_question['optionA']}
B. {current_user_question['optionB']}
C. {current_user_question['optionC']}
D. {current_user_question['optionD']}

分析:
{assistant_answer_1}

请输出本题正确答案的选项标号(A/B/C/D):
'''
            message_2 = [
                {"role": "system", "content": system_prompt_2},
                {"role": "user", "content": user_prompt_2}
            ]
            assistant_answer_2 = self.run_inference_llm(message_2)
            #print("选择题第二次回答:{}".format(assistant_answer_2))
            system_prompt_3 = "你是一个格式取消专家"
            # user_prompt_3 = f"请删除输入文本中的符号”.“，其他汉字及单词，并返回仅保留一个大写字母的纯文本。下面是一些示例：" \
            #                 f"（1）输入文本：“'A.'”，返回单个纯文本字母：“A“" \
            #                 f"（2）输入文本：“'D.'“。返回单个纯文本字母：“D“" \
            #                 f"（3）输入文本：“'C.'数据定义“。返回单个纯文本字母：“C“" \
            #                 f"现在，请对如下的输入文本提取SQL命令：{assistant_answer_2}"
            user_prompt_3 = f'''###请删除输入文本中的符号”.“，其他汉字及单词，并返回仅保留一个大写字母的纯文本。下面是一些示例：
示例1:
输入文本：'A.'，
返回单个纯文本字母：A

示例2:
输入文本：'D.'，
返回单个纯文本字母：D

示例3:
输入文本：B.，
返回单个纯文本字母：B

现在请返回下面输入文本的单个纯文本字母:

输入文本：{assistant_answer_2}

返回单个纯文本字母：
'''
            messages = [
                {"role": "system", "content": system_prompt_3},
                {"role": "user", "content": user_prompt_3}
            ]

        elif question_type == 'true_false_question':
            system_prompt_1 = "你是SQL方面的专家。请只考虑标准 SQL 的严格规定,根据提供的示例生成输出。"
            user_prompt_1 = f'''###逐步分析以下SQL相关的判断题，明确给出答案（True 或者 False）。请提供详细的4步的分析过程。
                                ###请参考我提供的示例的分析过程和格式，给出你的答案。
            
示例1:     
判断题：“使用 LIKE 运算符时，可以使用 % 来匹配任意字符的任意数量。”

分析步骤:
1. **确认题目陈述：**
   - 使用 LIKE 运算符时，可以使用 % 来匹配任意字符的任意数量。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - LIKE 运算符用于在 SQL 中进行模式匹配。
   - % 是一个通配符，用于匹配任意数量的字符，包括零个字符。

3. 逐步分析题目是否正确：
   - 在 SQL 中，LIKE 运算符确实可以用于进行模式匹配。
   - % 作为通配符，在 LIKE 运算符中使用时，可以匹配任意数量的字符，包括零个字符。
   - 因此，陈述“使用 LIKE 运算符时，可以使用 % 来匹配任意字符的任意数量”是正确的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是正确的。因此，答案是：True


示例2:
判断题：“在 SQL 中，所有函数都可以在 SELECT 语句中使用。”

分析步骤：

1. 确认题目陈述：
   - 在 SQL 中，所有函数都可以在 SELECT 语句中使用。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - SELECT 语句用于从数据库中查询数据。
   - 有多种类型的 SQL 函数，包括聚合函数（如 COUNT、SUM、AVG）、标量函数（如 UPPER、LOWER、LEN）、日期函数（如 NOW、CURDATE）等。
   - 并不是所有的 SQL 函数都适合在 SELECT 语句中使用。例如，DDL 函数（如 CREATE、ALTER）和某些系统函数并不能在 SELECT 语句中使用。

3. 逐步分析题目是否正确：
   - 聚合函数、标量函数和日期函数等常见的 SQL 函数通常可以在 SELECT 语句中使用。
   - 然而，DDL 操作（如 CREATE、ALTER）和某些系统函数不能在 SELECT 语句中使用。
   - 因此，陈述“在 SQL 中，所有函数都可以在 SELECT 语句中使用”是过于笼统和不准确的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是错误的。因此，答案是：False

示例3:
判断题：“一条 INSERT INTO 语句可以同时向多个表插入数据。”

分析步骤：

1. 确认题目陈述：
   - 一条 INSERT INTO 语句可以同时向多个表插入数据。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - INSERT INTO 语句用于向表中插入数据。
   - 标准 SQL 不支持一条 INSERT INTO 语句同时向多个表插入数据。

3. 逐步分析题目是否正确：
   - INSERT INTO 语句的标准用法是向一个表中插入数据。
   - 如果需要向多个表插入数据，需要使用多条 INSERT INTO 语句分别插入。
   - 因此，陈述“一条 INSERT INTO 语句可以同时向多个表插入数据”是错误的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是错误的。因此，答案是：False

示例4:
判断题：“SQL 中，EXISTS 运算符用于测试子查询是否返回结果。”

分析步骤：

1. 确认题目陈述：
   - SQL 中，EXISTS 运算符用于测试子查询是否返回结果。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - EXISTS 运算符用于检查子查询是否返回至少一行结果。
   - 如果子查询返回结果，EXISTS 运算符返回 TRUE；如果子查询不返回结果，则返回 FALSE。

3. 逐步分析题目是否正确：
   - EXISTS 运算符确实用于测试子查询是否返回结果。
   - 它常用于条件判断，确保某个条件是否存在。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是正确的。因此，答案是：True

示例5:
判断题：“在 SQL 中，DELETE 语句会删除表结构和数据。”

分析步骤：

1. 确认题目陈述：
   - 在 SQL 中，DELETE 语句会删除表结构和数据。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - DELETE 语句用于删除表中的数据，但不会删除表的结构。
   - DROP TABLE 语句用于删除表的结构和数据。
   - TRUNCATE 语句也删除表中的所有数据，但保留表结构。

3. 逐步分析题目是否正确：
   - DELETE 语句确实会删除表中的数据。
   - DELETE 语句不会删除表的结构。
   - 因此，陈述“在 SQL 中，DELETE 语句会删除表结构和数据”是错误的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是错误的。因此，答案是：False

示例6:
判断题：”所有 SQL 语句都可以嵌套在其他 SQL 语句中。“

分析步骤：

1. 确认题目陈述：
   - 所有 SQL 语句都可以嵌套在其他 SQL 语句中。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - 嵌套查询是指一个 SQL 查询嵌套在另一个 SQL 查询中，通常用于子查询。
   - 常见的嵌套查询包括 SELECT 语句中的子查询。
   - 并不是所有的 SQL 语句都可以嵌套在其他 SQL 语句中。例如，DDL 语句（如 CREATE TABLE、ALTER TABLE）和一些 DML 语句（如 INSERT、UPDATE）通常不能嵌套在其他 SQL 语句中。

3. 逐步分析题目是否正确：
   - SELECT 语句可以嵌套在其他 SELECT 语句中，这是常见的子查询用法。
   - 然而，DDL 语句如 CREATE TABLE 和 ALTER TABLE 不能嵌套在其他 SQL 语句中。
   - 类似地，INSERT、UPDATE、DELETE 等 DML 语句也不常作为嵌套语句使用。
   - 因此，陈述“所有 SQL 语句都可以嵌套在其他 SQL 语句中”是过于笼统和不准确的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是错误的。因此，答案是：False

示例7:
判断题：”在 SQL 中，所有列都可以设置为主键（PRIMARY KEY）。“

分析步骤：

1. 确认题目陈述：
   - 在 SQL 中，所有列都可以设置为主键（PRIMARY KEY）。

2. 提供与题目相关的 SQL 知识：
   - SQL 是结构化查询语言，用于管理和操作关系数据库。
   - PRIMARY KEY 是用于唯一标识表中每一行的列或一组列。
   - PRIMARY KEY 列必须满足两个条件：不包含 NULL 值，且其值必须唯一。
   - 并不是所有的列都适合作为 PRIMARY KEY。例如，包含重复值或 NULL 值的列不能设置为 PRIMARY KEY。
   - 例如，具有重复数据的列（如姓名列）或允许 NULL 值的列都不能作为 PRIMARY KEY。

3. 逐步分析题目是否正确：
   - PRIMARY KEY 列必须是唯一的，且不能包含 NULL 值。
   - 不是所有的列都能满足这些条件。例如，文本描述列、具有重复值的列或包含 NULL 值的列都不能设置为 PRIMARY KEY。
   - 因此，陈述“在 SQL 中，所有列都可以设置为主键（PRIMARY KEY）”是过于笼统和不准确的。

4. 给出最终结论（True 或 False）：
   - 基于以上分析，这个陈述是错误的。因此，答案是：False


判断题：“{user_question}”



分析步骤：
'''
            #print(f"true_false_question第一次user_prompt_1的分词数量: {len(word_tokenize(user_prompt_1))}")
            messages = [
                {"role":"system","content":system_prompt_1},
                {"role":"user","content":user_prompt_1}
            ]
            assistant_answer_1 = self.run_inference_llm(messages)
            #print("判断题第一次回答:{}".format(assistant_answer_1))
            system_prompt_2 = '你是一个结果提取专家'
            user_prompt_2 = f'从文本中提取出题目的最终判断结果'\
                            f'输入文本：”答案是：False.“'\
                            f'回答：”False“'\
                            f'输入文本：”答案是：True.“'\
                            f'回答：”True“'\
                            f'基于你对判断题：{user_question}的分析：\n'\
                            f'{assistant_answer_1}\n，请仅直接给出我答案(True/False)，Partially True被认为False：'
            messages = [
                {"role":"system","content":system_prompt_2},
                {"role":"user","content":user_prompt_2}
            ]
            assistant_answer_2 = self.run_inference_llm(messages)
            #print("判断题第二次回答:{}".format(assistant_answer_2))
            #print(f"true_false_question第二次user_prompt_2的分词数量: {len(word_tokenize(user_prompt_2))}")
            system_prompt_3 = "你是一个格式取消专家"
            user_prompt_3 = f"请删除输入文本中的符号”.“以及其他文本，并返回仅保留True/False的纯文本。下面是一些示例："\
                            f"（1）输入文本：“True.”，返回单个纯文本字母：“True“"\
                            f"（2）输入文本：“False.“。返回单个纯文本字母：“False“" \
                            f"（3）输入文本：“Partially True.“。返回单个纯文本字母：“False“"\
                            f"现在，请对如下的输入文本提取SQL命令：{assistant_answer_2}"
            # messages.append({"role": "assistant", "content": assistant_answer_2})
            messages.append({"role":"system","content":system_prompt_3})
            messages.append({"role":"user","content":user_prompt_3})
            #print(f"true_false_question第三次user_prompt_3的分词数量: {len(word_tokenize(user_prompt_3))}")
        return messages  # noqa

    # 此方法会被跑分服务器调用， messages 选手的 construct_prompt() 返回的结果
    # 请不要对此函数做任何改动
    def run_inference_llm(self, messages):
        pass
