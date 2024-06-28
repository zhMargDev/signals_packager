import psycopg2, telebot, json, os, hashlib, ast
import signal_template as st
import folders_regions as fr
import configs.config as config


from datetime import datetime 
import os

# Получить текущий каталог скрипта
current_dir = os.path.dirname(__file__)

# Создать абсолютный путь к файлу
file_path = os.path.join(current_dir, 'configs', 'rate.json')

with open(file_path, 'r') as f:
    rate_data = json.load(f)


conn = psycopg2.connect(
                host='127.0.0.1',
                port="5432",
                database='signals_parser',
                user='parser',
                password='parser'
            )
cursor = conn.cursor()

async def insert_packaged_signals(data, conn, cursor, region):
    add_to_ps="INSERT INTO packaged_signals (region , region_number , channel_type , signal_channel_id , signal_message_id , date  ) VALUES (%s , %s , %s , %s , %s , %s)"
    ps_values = (data[0], data[1], data[2], data[3], data[4], data[5])
    cursor.execute(add_to_ps, ps_values)    
    conn.commit()
    await bot_function(cursor , data[3], data[4] , data[2] , region)
    
async def hash_to_8_chars(input_string):
        # Преобразование входных данных в строку (если это число)
        if isinstance(input_string, int):
            input_string = str(input_string)
        
        # Вычисление SHA-256 хеша
        sha256_hash = hashlib.sha256(input_string.encode()).hexdigest()
        
        # Возвращение первых 8 символов хеша
        return sha256_hash[:8]

async def bot_function( cursor , channel_id , message_id, channel_type, region):

    bot = telebot.TeleBot( config.RU_GROUPS[str(rate_data[region])]['BOT'])
    cursor.execute(f'SELECT * FROM signals WHERE channel_id = {channel_id} AND message_id = {message_id}' )
    signal = cursor.fetchall()[0]

    # Получение хеша
    channel_hashed_id = await hash_to_8_chars(signal[0])

    data = {
        'channel_id': channel_hashed_id,
        'channel_name': signal[3],
        'trend': signal[7],
        'coin': signal[6],
        'margin': signal[14],
        'leverage': signal[13],
        'tvh': signal[8],
        'lvh': ast.literal_eval(signal[10]),   # Преобразование строки в массив
        'rvh': signal[9],
        'targets': ast.literal_eval(signal[11]),  # Преобразование строки в массив
        'stop_less': signal[12]
    }

    path = f'groups/ru_{rate_data[region]}/signal.html'
    # Проверка существования файла
    if not os.path.exists(path):
        path = 'groups/signal.html'
    lite_path = f'groups/ru_{rate_data[region]}/lite_blocked.html'
    # Проверка существования файла
    if not os.path.exists(lite_path):
        lite_path = 'groups/lite_blocked.html'

    premium_path = f'groups/ru_{rate_data[region]}/premium_blocked.html'
    # Проверка существования файла
    if not os.path.exists(premium_path):
        premium_path = 'groups/premium_blocked.html'
        
    sended_message =  await st.signal_template_format(data, path)
    lite_blocked_message =  await st.signal_blocked_template_format(data, lite_path)
    blocked_message =  await st.signal_blocked_template_format(data, premium_path)
    
    if channel_type == 'LITE':
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['LITE'], sended_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['PREMIUM'],sended_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')
    elif channel_type == 'PREMIUM':
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['LITE'], lite_blocked_message,  parse_mode='HTML' )
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['PREMIUM'], sended_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')
    else:
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['LITE'], lite_blocked_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['PREMIUM'], blocked_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(rate_data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')

    # Завершаем работу бота
    return
        

async def package_by_channels(chanel_id, message_id, region):
    current_datetime = datetime.now()
    current_date = current_datetime.date()
    date_str = current_date.strftime("%Y-%m-%d")
    conn = psycopg2.connect(
                host='127.0.0.1',
                port="5432",
                database='signals_parser',
                user='parser',
                password='parser'
            )
    cursor = conn.cursor()
    rate = rate_data[region]

    sql = "SELECT * FROM packaged_signals WHERE date = %s AND region = %s AND region_number = %s"
    cursor.execute(sql, (date_str, region, str(rate)))
    signals=cursor.fetchall()
    lite_signals=0
    premium_signals=0
    vip_signals=0
    
    for signal in signals:
        if signal[3] == 'LITE' : lite_signals += 1
        elif signal[3] == 'PREMIUM': premium_signals += 1
        elif signal[3] == 'VIP': vip_signals += 1


 
    if lite_signals == 0 :
        await insert_packaged_signals([region, str(rate_data[region]), 'LITE', chanel_id, message_id, date_str], conn , cursor, region)
        return 0
    elif lite_signals< config.RU_GROUPS[str(rate_data[region])]["LITE_SIGNAL_COUNT"]:
        lite_signals_flag = 0
        lite_count = 0
        for signal in signals:
            if signal[3] == 'LITE' and lite_signals_flag != lite_signals:
                lite_signals_flag += 1
            elif lite_signals_flag == lite_signals: 
                lite_count += 1
                if lite_count== config.RU_GROUPS[str(rate_data[region])]["LITE_SIGNAL_PROBELS"]:
                    await insert_packaged_signals([region, str(rate_data[region]), 'LITE', chanel_id, message_id, date_str], conn , cursor, region)
                    return 0

    if premium_signals == 0 :
        await insert_packaged_signals([region, str(rate_data[region]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor, region)
        return 0
    elif premium_signals< config.RU_GROUPS[str(rate_data[region])]["PREMIUM_SIGNAL_COUNT"]:
        premium_signals_flag = 0
        premium_count = 0
        for signal in signals:
            if signal[3] == 'PREMIUM' and premium_signals_flag != premium_signals:
                premium_signals_flag  += 1
            elif premium_signals_flag  == premium_signals: 
                premium_count += 1
                if premium_count==  config.RU_GROUPS[str(rate_data[region])]["PREMIUM_SIGNAL_PROBELS"]:
                    await insert_packaged_signals([region, str(rate_data[region]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor, region)
                    return 0                
    if vip_signals == config.RU_GROUPS[str(rate_data[region])]["VIP_SIGNAL_COUNT"]: return 
        
    await insert_packaged_signals([region, str(rate_data[region]), 'VIP', chanel_id, message_id, date_str], conn , cursor, region)
    
                                                                                                                 

