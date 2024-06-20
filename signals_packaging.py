import psycopg2
import config 
import telebot
import json

from datetime import datetime 

rate_json_file = 'rate.json'
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

async def insert_packaged_signals(data, conn, cursor):
    add_to_ps="INSERT INTO packaged_signals (region , region_number , channel_type , signal_channel_id , signal_message_id , date  ) VALUES (%s , %s , %s , %s , %s , %s)"
    ps_values = (data[0], data[1], data[2], data[3], data[4], data[5])
    cursor.execute(add_to_ps, ps_values)    
    conn.commit()
    await bot_function(cursor , data[3], data[4] , data[2] )
    
async def bot_function( cursor , channel_id , message_id, channel_type):

    bot = telebot.TeleBot( config.RU_GROUPS[str(data["ru_rate"])]['BOT'])
    cursor.execute(f'SELECT * FROM signals WHERE channel_id = {channel_id} AND message_id = {message_id}' )
    signal = cursor.fetchall()[0]

    tvh = ''
    if signal[8] != 'False': tvh = f'Tvh: {signal[8]}'

    rvh = ''
    if signal[9]: rvh = 'Rvh'

    lvh_message = ''
    
    if signal[10] != []:
        flag = 1
        for number in eval(signal[10]):
            lvh_message += f'{" " * 24}Lvh{flag}: {number}\n'
            flag += 1

    tp_message = ''
    flag = 1
    for number in eval(signal[11]):
        tp_message += f'{" " * 24}Tp{flag}: {number}\n'
        flag += 1

    stop_less = ''
    if signal[12] != 'Def': stop_less = f'Stop: {signal[12]}'

    leverage = ''
    if signal[13] != 'Def': leverage = f"Leverage: {signal[13]}x"

    sended_message = f'''
    ID: {signal[0]}
    {signal[3]}
    
    Trend: {signal[7]}    |    Coin: {signal[6]}
    Margin:  {signal[14]}    |    Leverage:  {leverage}
        
    Entry:
              {tvh}
              {rvh}
              {lvh_message}
    Target:
              {tp_message} 

    Stop:     {stop_less}

    '''
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
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['LITE'], sended_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['PREMIUM'], blocked_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['VIP'], sended_message)
    elif channel_type == 'PREMIUM':
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['LITE'], lite_blocked_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['PREMIUM'], sended_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['VIP'], sended_message)
    else:
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['LITE'], lite_blocked_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['PREMIUM'], blocked_message)
        bot.send_message(config.RU_GROUPS[str(data["ru_rate"])]['CHANNELS']['VIP'], sended_message)

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
    ru_rate=str(data["ru_rate"])
    sql = f"SELECT * FROM packaged_signals WHERE date == {date_str} AND region == 'RU' AND region_number == {ru_rate}"
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
        await insert_packaged_signals(['RU', str(data["ru_rate"]), 'LITE', chanel_id, message_id, date_str], conn , cursor)
        return 0
    elif lite_signals< config.RU_GROUPS[str(data["ru_rate"])]["LITE_SIGNAL_COUNT"]:
        lite_signals_flag = 0
        lite_count = 0
        for signal in signals:
            if signal[3] == 'LITE' and lite_signals_flag != lite_signals:
                lite_signals_flag += 1
            elif lite_signals_flag == lite_signals: 
                lite_count += 1
                if lite_count== config.RU_GROUPS[str(data["ru_rate"])]["LITE_SIGNAL_PROBELS"]:
                    await insert_packaged_signals(['RU', str(data["ru_rate"]), 'LITE', chanel_id, message_id, date_str], conn , cursor)
                    return 0

    if premium_signals == 0 :
        await insert_packaged_signals(['RU', str(data["ru_rate"]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor)
        return 0
    elif premium_signals< config.RU_GROUPS[str(data["ru_rate"])]["PREMIUM_SIGNAL_COUNT"]:
        premium_signals_flag = 0
        premium_count = 0
        for signal in signals:
            if signal[3] == 'PREMIUM' and premium_signals_flag != premium_signals:
                premium_signals_flag  += 1
            elif premium_signals_flag  == premium_signals: 
                premium_count += 1
                if premium_count==  config.RU_GROUPS[str(data["ru_rate"])]["PREMIUM_SIGNAL_PROBELS"]:
                    await insert_packaged_signals(['RU', str(data["ru_rate"]), 'PREMIUM', chanel_id, message_id, date_str], conn , cursor)
                    return 0                

    await insert_packaged_signals(['RU', str(data["ru_rate"]), 'VIP', chanel_id, message_id, date_str], conn , cursor)
    

    cursor.execute(signal , date_str)
    conn.commit()                                                                                                                                         

