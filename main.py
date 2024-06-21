import psycopg2, json, ccxt, asyncio, signals_packaging
import time as timer
import configs.config as config
import folders_regions as fr

from datetime import datetime, timedelta

rate_json_file = 'configs/rate.json'
with open(rate_json_file, 'r') as f:
    rate_data = json.load(f)


async def get_folder_region(signal_channel_id, cursor):
    # Полученаем id папки которому принодлежит канал данного сигнала
    cursor.execute(f'SELECT * FROM channels WHERE channel_id = {signal_channel_id}')
    channels = cursor.fetchall()
    if len(channels != 0):
        channel = channels[0]
        folder_id = channel[1]

        # Получаем название папки
        cursor.execute(f'SELECT * FROM folders WHERE folder_id = {folder_id}')
        folders = cursor.fatchall()
        if len(folders) != 0:
            folder = folders[0]
            return folder[2]
        else:
            return 'Err'
    else:
        return 'Err'

# Функцыя которая проверяет сыгналы, те сигналы которые не прошли проверку перекидываются
async def defecte_signals(signal_id, conn, cursor, message):
    sql = 'INSERT INTO defecte_signals (signal_id , message) VALUES (%s, %s)'
    values = (signal_id, message)
    cursor.execute(sql, values)
    conn.commit()
    return True

# Функцыя которя расшитивает процент подения/повышения 
async def check_differnce(price, signal_price, config, signal_id, conn, cursor):
    if signal_price < 0: return False
    procent = (price*config.PRICE_DIFFERENCE)/100  
    if abs(signal_price - price) > procent:
        message = 'Сигнал не являетса актуальным .'
        await defecte_signals(signal_id, conn, cursor, message)
        return False
    else: return True

    
async def main():
    # Создание экземпляра объекта для биржи Binance
    exchange = ccxt.binance()
    while True:
        try:    
            conn = psycopg2.connect(
                host='127.0.0.1',
                port="5432",
                database='signals_parser',
                user='parser',
                password='parser'
            )
            cursor = conn.cursor()

            # Получение текущей даты и времени и вычисление 5 минут
            current_datetime = datetime.now() - timedelta(minutes=5)

            # Разделение на дату и время
            current_date = current_datetime.date()
            current_time = current_datetime.time()

            # Преобразование в строку
            date_str = current_date.strftime("%Y-%m-%d")
            time_str = current_time.strftime("%H:%M:%S")

            # Получаем сегодняшние сигналы за последние 5 минут 
            sql_query = "SELECT * FROM signals WHERE date = %s AND time >= %s"
            cursor.execute(sql_query, (date_str, time_str ))
            signals = cursor.fetchall()

            

            # Цыкл, который поочередно берет сыгналы и проверяет их
            for signal in signals:
                region = await get_folder_region(signal[1], cursor)

            # Проверка существует ли такой сигнал в проверенных сигналах
                check_signals = "SELECT * FROM verified_signals WHERE channel_id = %s AND message_id = %s"
                cursor.execute(check_signals, (signal[1], signal[2]))
                exist_signal = cursor.fetchall()
                if len(exist_signal) != 0: continue
            # Проверка существует ли такой сигнал в проверенных сигналах
                def_signals = f"SELECT * FROM defecte_signals WHERE signal_id = {int(signal[0])}"
                cursor.execute(def_signals)
                exist_signal = cursor.fetchall()
                if len(exist_signal) != 0: continue
                
                
                coin = signal[6]
                trend = signal[7]

            # Проверка Тренда, если тренд не совпадает, перекидывает Сигнал
                if trend not in ['LONG', 'SHORT']: 
                    message = 'Тренд не прошел проверку.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue

            # Проверка TVH, если TVH пустой или там нет цыфр, перекидывает Сигнал , а если есть , то Сравнивается  процентный перепад  
                tvh =  signal[8]
                if tvh == "False" and not bool(rvh) and len(lvh) == 0 :
                    message = ' Не являетса сигналом.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue
                try: 
                    if tvh != 'False': tvh = float(tvh)
                except:
                    message = ' TVH не прошел проверку.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue
                
                rvh = signal[9]

            # Меняем тип LVH, если LVH не становтся цыфрой , перекидывает Сигнал    
                lvh_str_type = eval(signal[10])
                lvh = []
                try:
                    for number in lvh_str_type: lvh.append(float(number))
                except:
                    message = 'LVH не прошел проверку.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue

                targets_string_type = eval(signal[11])
                targets = []
                try:
                    for target in targets_string_type: targets.append(float(target))
                except:
                    message = 'Таргеты не прошели проверку.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue
                
            # Проверка Таргетов , если есть таргет , то от сравнивается с TVH и LVH      
                if 'Def' not in targets:
                    for target in targets:
                        if trend == 'LONG':
                            if tvh != 'False' and float(target) < float(tvh):
                                message = 'таргет не прошел проверку.'
                                flag =await defecte_signals(signal[0], conn, cursor, message)
                                if flag: continue
                        else:  
                            if tvh != 'False' and float(target) > float(tvh):
                                message = 'таргет не прошел проверку.'
                                flag =await defecte_signals(signal[0], conn, cursor, message)
                                if flag: continue
                
                
            # Праверка Стоп Сигнала , если он есть , то сравнивается с Таргетом , TVH или LVH
                stop_loss =  signal[12]
                if 'Def' not in stop_loss: 
                    stop_loss = float(stop_loss)
                    if trend == 'LONG' and stop_loss > target: stop_loss=""
                    elif tvh != '' and stop_loss > tvh: stop_loss=""
                    elif lvh!=[] and stop_loss > lvh: stop_loss=""
                        
                price = 0

            # Проверка существет ли коин , если есть , Получение текущей цены для пары Coin/USDT
                try:
                    ticker = exchange.fetch_ticker(f'{coin}/USDT')
                    price = ticker['last']
                    
                except:
                    message = 'Валюта не является настоящей.'
                    flag =await defecte_signals(signal[0], conn, cursor, message)
                    if flag: continue
                
                
               

            # Проверка TVH, если TVH не пустой , Сравнивается процентный перепад    
                flag =await check_differnce(price, tvh, config, signal[0], conn, cursor)
                if not flag: continue

            # Проверка LVH, если LVH не пустой , Сравнивается процентный перепад   
                if len(lvh) != 0:
                    for lvh_price in lvh:
                        flag =await check_differnce(price, lvh_price, config, signal[0], conn, cursor)
                        if not flag: continue
                    
            # Проверка Таргетов, если таргет не пустой , Сравнивается процентный перепад             
                if len(targets) != 0:
                    for tp_price in targets:
                        flag =await check_differnce(price, tp_price, config, signal[0], conn, cursor)
                        if not flag: continue

                add_to_db = "INSERT INTO verified_signals (channel_id, message_id, channel_name, date, time, coin, trend, tvh, rvh, lvh, targets, stop_less, leverage, margin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (signal[1], signal[2], signal[3], signal[4], signal[5], signal[6], signal[7], signal[8], signal[9], signal[10], signal[11], signal[12], signal[13], signal[14])
                cursor.execute(add_to_db, values)
                conn.commit()

                await signals_packaging.package_by_channels(signal[1], signal[2], region)
                cursor.execute('SELECT * FROM verified_signals')
                
                rate_json_file = 'configs/rate.json'
                with open(rate_json_file, 'r') as f:
                    rate_data = json.load(f)

                # Изменяем значение ru_rate
                rate_data[region] += 1
                if rate_data[region] > len(config.RU_GROUPS):
                    rate_data[region] = 1

                # Записываем изменённые данные обратно в файл rate.json
                with open(rate_json_file, 'w') as f:
                    json.dump(rate_data, f, indent=4)

            # Закрытие курсора и соединения
            cursor.close()
            conn.close()
            timer.sleep(5)
        except:
            timer.sleep(5)
            
if __name__ == "__main__":
    asyncio.run(main()) 