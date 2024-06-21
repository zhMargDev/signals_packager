import psycopg2, telebot, json, os, hashlib
import signal_template as st
import folders_regions as fr
import configs.config as config

from datetime import datetime 

rate_json_file = 'configs/rate.json'
with open(rate_json_file, 'r') as f:
    data = json.load(f)


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

    bot = telebot.TeleBot( config.RU_GROUPS[str(data[region])]['BOT'])
    cursor.execute(f'SELECT * FROM signals WHERE channel_id = {channel_id} AND message_id = {message_id}' )
    signal = cursor.fetchall()[0]

    # Получение хеша
    channel_hashed_id = await hash_to_8_chars(signal[0])

    data = {
        'channel_id': channel_hashed_id,
        'channel_name': signal[3],
        'trend' : signal[7],
        'coin': signal[6],
        'margin': signal[14],
        'leverage' : signal[13],
        'tvh' : signal[8],
        'lvh' : signal[10],
        'rvh' : signal[9],
        'targets' : signal[11],
        'stop_less': signal[12]
    }

    path = f'groups/ru_{channel_id}/signal.html'
    # Проверка существования файла
    if not os.path.exists(path):
        path = 'groups/signal.html'
        
    sended_message =  await st.signal_template_format(data, path)

    blocked_message = f'''
    Чтобы посмотреть все сигналы вы можете купить подписку на VIP

    ID:  {signal[0]}
    {signal[3]}
    
    Trend: {signal[7]}    |    Coin: {signal[6]}
    Margin:  Def   |    Leverage: Def
        
    Entry:
              [VIP]
              [VIP]
              [VIP]
    Target:
              [VIP]

    Stop:     [VIP]

    '''

    lite_blocked_message = f'''
    Чтобы посмотреть все сигналы вы можете купить подписку на PREMIUM или VIP

    ID:  {signal[0]}
    {signal[3]}
    
    Trend: {signal[7]}    |    Coin: {signal[6]}
    Margin:  Def   |    Leverage: Def
        
    Entry:
              [PREMIUM / VIP]
              [PREMIUM / VIP]
              [PREMIUM / VIP]
    Target:
              [PREMIUM / VIP]

    Stop:     [PREMIUM / VIP]

    '''
    if channel_type == 'LITE':
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['LITE'], sended_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['PREMIUM'], blocked_message)
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')
    elif channel_type == 'PREMIUM':
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['LITE'], lite_blocked_message)
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['PREMIUM'], sended_message,  parse_mode='HTML')
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')
    else:
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['LITE'], lite_blocked_message)
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['PREMIUM'], blocked_message)
        bot.send_message(config.RU_GROUPS[str(data[region])]['CHANNELS']['VIP'], sended_message,  parse_mode='HTML')

    # Завершаем работу бота
    return
        

async def package_by_channels(chanel_id, message_id):
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
    region = await get_folder_region(chanel_id, cursor)
    rate = data[region]

    sql = f"SELECT * FROM packaged_signals WHERE date == {date_str} AND region == {region} AND region_number == {rate}"
    cursor.execute(sql)
    signals=cursor.fetchall()
    lite_signals=0
    premium_signals=0
    vip_signals=0
    
    for signal in signals:
        if signal[3] == 'LITE' : lite_signals += 1
        elif signal[3] == 'PREMIUM': premium_signals += 1
        elif signal[3] == 'VIP': vip_signals += 1


 
    if lite_signals == 0 :
        await insert_packaged_signals([region, str(data[region]), 'LITE', chanel_id, message_id, date_str], conn , cursor, region)
        return 0
    elif lite_signals< config.RU_GROUPS[str(data[region])]["LITE_SIGNAL_COUNT"]:
        lite_signals_flag = 0
        lite_count = 0
        for signal in signals:
            if signal[3] == 'LITE' and lite_signals_flag != lite_signals:
                lite_signals_flag += 1
            elif lite_signals_flag == lite_signals: 
                lite_count += 1
                if lite_count== config.RU_GROUPS[str(data[region])]["LITE_SIGNAL_PROBELS"]:
                    await insert_packaged_signals([region, str(data[region]), 'LITE', chanel_id, message_id, date_str], conn , cursor, region)
                    return 0

    if premium_signals == 0 :
        await insert_packaged_signals([region, str(data[region]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor, region)
        return 0
    elif premium_signals< config.RU_GROUPS[str(data[region])]["PREMIUM_SIGNAL_COUNT"]:
        premium_signals_flag = 0
        premium_count = 0
        for signal in signals:
            if signal[3] == 'PREMIUM' and premium_signals_flag != premium_signals:
                premium_signals_flag  += 1
            elif premium_signals_flag  == premium_signals: 
                premium_count += 1
                if premium_count==  config.RU_GROUPS[str(data[region])]["PREMIUM_SIGNAL_PROBELS"]:
                    await insert_packaged_signals([region, str(data[region]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor, region)
                    return 0                

    await insert_packaged_signals([region, str(data[region]), 'VIP', chanel_id, message_id, date_str], conn , cursor, region)
    

    cursor.execute(signal , date_str)
    conn.commit()                                                                                                                                         

