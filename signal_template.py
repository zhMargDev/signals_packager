from bs4 import BeautifulSoup

"""
    Данный файл получает данные сигнала которые надо отправить в канал
    Формирует в макет и отправляет назад уже нужжные данные в макете для отправки в канал
"""

async def signal_template_format(signal, path):
    print("MONITOR - ", signal)

    """
        Данная фукнция получает макет из html файла, добавляет туда нужные данные и отправляет обатно
    """
    # Получаем макет html файла
    with open(path, 'r') as f:
        template_file = f.read()

    # Переобразуем в суп файл для работы с ним
    soup = BeautifulSoup(template_file, 'html.parser')
    # Находим body в котором находится весь наш теймплейт
    template = soup.find('body')

    # Проверяем если есть тег channel_id то добавляем в него id канала
    if template.find('channel_id') is not None:
        template.find('channel_id').string = str(signal['channel_id'])
    
    # Проверяем если есть тег channel_name То добавляем в него название канала
    if template.find('channel_name') is not None:
        template.find('channel_name').string = str(signal['channel_name'])

    # Проверяем если есть тег trend то добавляем туда значения trend
    if template.find('trend') is not None:
        template.find('trend').string = str(signal['trend'])
        # Проверяем trend если лонг то добавляем икноку ⬆️, если шорт то добавляем  ⬇️
        if template.find('trend_icon') is not None:
            if str(signal['trend']).lower() == 'long':
                template.find('trend_icon').string = '⬆️'
            elif str(signal['trend']).lower() == 'short':
                template.find('trend_icon').string = '⬇️'
    
    # Проверяем есть ли тег margin и добавляем туда маржу
    if template.find('margin') is not None:
        template.find('margin').string = str(signal['margin'])

    # Проверяем есть ли тег leverage и добавляем туда плечо в цормате '0x'
    if template.find('leverage') is not None:
        if signal['leverage'] == 'Def': template.find('leverage').string = f"{signal['leverage']}"
        else: template.find('leverage').string = f"{signal['leverage']}x"

    # Проверяем есть ли тег coin  и добавляем туда тип коина
    if template.find('coin') is not None:
        template.find('coin').string = str(signal['coin'])

    # Проверяем какой у нас значения tvh rvh lvh
    # Если твх это цира то находим тег tvh и добавляем в него
    if 'False' not in str(signal['tvh']):
        if template.find('entry') is not None:
            template.find('entry').append(f"\n{' ' * 24}tvh: {signal['tvh']}")

    # Проверяем если наш rvh это тру то добавляем рвх
    if 'True' in str(signal['rvh']):
        if template.find('entry') is not None:
            template.find('entry').append(f'\n{" " * 24}rvh')

    # Проверяем пустой ли lvh, если нет то формируем форму и добавляем в форму сигнала
    if len(signal['lvh']) != 0 and template.find('entry') is not None:
        for el in signal['lvh']:
            lvh_tag = soup.new_tag('p')
            lvh_tag.string = f'\n{" " * 24}lvh{signal["lvh"].index(el) + 1}: {el}'
            lvh_tag.append(soup.new_tag('br'))
            template.find('entry').append(lvh_tag)

    # Проверяем если первое значение таргет это Def то добавляем его в макет
    try:
        if not signal["targets"] or len(signal["targets"]) != 0 and 'def' in str(signal["targets"][0]).lower():
            if template.find('tp') is not None:
                template.find('tp').string = 'Def'
        else:
            # Если же первое значение не деф значит в нём есть значение
            # Формируем форму целей и добавляем в тег форму сигнала если тег tp присутсувует
            if template.find('tp') is not None:
                index_flag = 0
                for el in signal["targets"]:
                    tp_tag = soup.new_tag('p')
                    tp_tag.string = f'tp{signal["targets"].index(el) + 1}: {el} \n'
                    if index_flag == 1:
                        # Если это не первый элемент то добавить перед ним 24 пробело что обеспечит 24 отступов
                        template.find('tp').append(" " * 24)
                    template.find('tp').append(tp_tag)
                    index_flag = 1
    except:
        pass
    # Находим тег стоп и добавляем в него стоп лесс (если стоп деф то добавиться деф)
    if template.find('stop') is not None:
        template.find('stop').string = str(signal['stop_less'])

    return template.text