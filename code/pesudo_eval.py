from submit import submission
import json
from openai import OpenAI
import requests

# 本代码为测试用例，选手可自己调用本地大语言模型接口进行测试，具体的格式为OpenAI格式的接口形式
openai_api_key = ""  #请参照openai api使用文档，按照实际填写
openai_api_base = "https://dashscope.aliyuncs.com/compatible-mode/v1"
client = OpenAI(api_key=openai_api_key,base_url=openai_api_base)


def evaluate_mcq(predicted_answer,label):
    print(f"选手Prompt生成的SQL:{predicted_answer},\n标准答案：{label}")
    return predicted_answer.upper().strip()==label.upper().strip()


def evaluate_sql(predicted_answer,label):
    print(
        "****该评估方法仅提供一个虚拟评测用例，仅提供给选手进行调试使用。实际评测时，正确性则由SQL执行结果是否正确的判断。****")
    print(f"选手Prompt生成的SQL:{predicted_answer},\n标准答案：{label}")
    return predicted_answer.strip()==label.strip()


class eval_submission(submission):
    def parse_table(self,table_meta_path):
        with open(table_meta_path,'r') as db_meta:
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

    def run_inference_llm(self,constructed_prompts):
        if isinstance(constructed_prompts,str):
            pass
        elif isinstance(constructed_prompts,list):
            pass
        elif isinstance(constructed_prompts,dict):
            pass
        messages = constructed_prompts
        # print(f"调用模型进行测试,用户的prompt为：{constructed_prompts}")
        llm_response = client.chat.completions.create(
            messages=messages,
            model="qwen1.5-14b-chat",  # 这里填写所选择的LLM名称，推荐使用Qwen1.5-14B-Chat模型进行线下开发评估
            max_tokens=1999,
            temperature=0.0,  #  temperature 实际跑分时默认设置为0.0，避免随机性
            stream=False
        )

        llm_outputs = llm_response.choices[0].message.content
        #print(llm_response)
        toekns_num = llm_response.usage.prompt_tokens
        print('本次输入的tokens为:{}'.format(toekns_num))
        return llm_outputs


if __name__=="__main__":
    user_submission = eval_submission(table_meta_path="样例数据/sample_tables.json")
    question_jsonl_filename = "样例数据/sample_question.jsonl"
    gt_jsonl_filename = "样例数据/sample_answer.jsonl"
    answer_dict = {}

    num_questions,num_correct_answer = 0,0
    with open(gt_jsonl_filename,'r',encoding='utf-8') as gt_file:
        for line in gt_file:
            data = json.loads(line)

            answer_dict[data['question_id']] = data['answer']
            num_questions += 1

    with open(question_jsonl_filename,'r',encoding='utf-8') as file:
        for line in file:
            data = json.loads(line)
            if data['question_type'] != 'multiple_choice':
                continue
            # if data['question_id']<7:
            #     continue

            print(data)

            message = user_submission.construct_prompt(data)
            response = user_submission.run_inference_llm(message)
            #print("LLM回复为:{}".format(response))
            question_id = data['question_id']
            question_type = data['question_type']

            if question_type=="text2sql":
                db_name = data['db_id']
                label = answer_dict[question_id]
                judge_result = evaluate_sql(response,label)

            if question_type=="multiple_choice":
                label = answer_dict[question_id]
                judge_result = evaluate_mcq(response,label)

            if question_type=="true_false_question":
                label = answer_dict[question_id]
                judge_result = evaluate_mcq(response,label)

            if judge_result:
                print("right")
                num_correct_answer += 1
            else:
                print("error")

    print(f"Accuracy:{num_correct_answer/num_questions}")