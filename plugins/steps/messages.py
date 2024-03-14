# plugins/steps/messages.py

from airflow.providers.telegram.hooks.telegram import TelegramHook
import os
from dotenv import load_dotenv


def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    load_dotenv()
    token = os.environ.get('TG_TOKEN')
    chat_id = os.environ.get('TG_CHAT_ID')
    
    hook = TelegramHook(telegram_conn_id='test',
                        token=token,
                        chat_id=chat_id)
    dag = context['ti']['dag']
    run_id = context['ti']['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' 
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    }) 


def send_telegram_failure_message(context):
    load_dotenv()
    token = os.environ.get('TG_TOKEN')
    chat_id = os.environ.get('TG_CHAT_ID')
    
    hook = TelegramHook(telegram_conn_id='test',
                        token=token,
                        chat_id=chat_id)
    
    task_instance_key_str = context['ti']['task_instance_key_str']
    run_id = context['ti']['run_id']
    
    message = f'Исполнение DAG {task_instance_key_str} с id={run_id} прошло неуспешно' 
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })
