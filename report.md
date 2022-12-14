# Отчёт о выполнении задачи "Delivery robot"

- [Отчёт о выполнении задачи "Delivery robot"](#отчёт-о-выполнении-задачи-secure-update)
  - [Постановка задачи](#постановка-задачи)
  - [Известные ограничения и вводные](#известные-ограничения-и-вводные)
    - [Цели и Предположения Безопасности (ЦПБ)](#цели-и-предположения-безопасности-цпб)
  - [Архитектура системы](#архитектура-системы)
    - [Компоненты](#компоненты)
    - [Алгоритм работы решения](#алгоритм-работы-решения)
    - [Описание Сценариев (последовательности выполнения операций), при которых ЦБ нарушаются](#описание-сценариев-последовательности-выполнения-операций-при-которых-цб-нарушаются)
    - [Указание "доверенных компонент" на архитектурной диаграмме с обоснованием выбора.](#указание-доверенных-компонент-на-архитектурной-диаграмме-с-обоснованием-выбора)
    - [Политики безопасности](#политики-безопасности)
  - [Запуск приложения и тестов](#запуск-приложения-и-тестов)
    - [Запуск приложения](#запуск-приложения)
    - [Запуск тестов](#запуск-тестов)

## Постановка задачи

Доставка заказа клиенту по указанным координатам (упрощение – доступно движение по прямой) при помощи движущегося робота и выдача заказа при условии совпадения места доставки и полученного/ожидаемого пароля (упрощение – клиенту известен правильный пинкод от заказа), возвращение на исходную позицию и завершение работы.

## Известные ограничения и вводные

По условиям организаторов должна использоваться микросервисная архитектура и шина обмена сообщениями для реализации асинхронной работы сервисов.

### Цели и Предположения Безопасности (ЦПБ)
Цели безопасности:
-	обеспечение сохранности груза до момента передачи авторизованному клиенту
-	получение задания только от корпоративного сервера
-	в случае многократных попыток неавторизованного доступа вернуть груз на склад
-	осуществлять проверку пинкода только по достижению указанной в заказе точке

Предположения безопасности:
-	защита от атак с использованием физического доступа к аппаратной составляющей
-	робот ездит по прямой без препятсвий 
- после получения задания робот функционирует автономно (т.е. движется к клиенту, авторизует и выдаёт заказ даже в отсутствие связи с сервером)
-	после доставки робот должен вернуться в пункт отправления, только тогда его задача считается выполненной

## Архитектура системы

### Компоненты

- **Client** абстрактный пользователь, чья задача ввести пинкод. Для этого в рамках реализуемой задачи он пользуется файлом request.rest для редактирования и отправки запросов.
- **Fleet management service (fleet)** абстрактный сотрудник выдает заказы роботу при помощи файла request.rest, получая на REST интерфейс уведомления о принятии заказа к выполнению и о результате доставки (т.е. в т.ч. и возвращении робота на базу).
- **Communication block (communication)** разворачивает REST API для приема заказа, передачи его далее по шине, а так же для передачи сообщений подтверждения и результата доставки.
- **HMI (hmi)** разворачивает REST API для приема заказа и передачи его далее по шине. Поскольку подразумевается автономное функционирование робота, то данный элемент считаем аппаратной составляющей со всеми вытекающими последствиями для безопасности.
- **Positioning (positioning)** осуществляет функции расчета параметров движения.
- **Sensors (sensors)** открывают замки при команде от монитора безопасности, через 5 секунд закрывает, отправляя об этом сообщение.
- **Motion control (motion)** осуществляет движение по полученным направлению и скорости в течении заданного расстояния с учетом аппаратных (?) ограничений. Раз в некоторое время/этап сообщает о прохождении заданного расстояния (в Central по факту) с применёнными параметрами движения в Positioning. В результате Central знает о достижении контрольной точки, а Positioning может посчитать новые координаты.
- **Security monitor (monitor)** читает все сообщения, проверяет на соответствие имеющихся политик безопасности, при успешном прохождении пропускает дальше.
- **Central Control Unit (central)** принимает заказ, сохраняет требуемую точку доставки и пинкод, запускает расчет и старт движения движение. При получении сообщения о достижении точки назначения проверяет по координатам соответствие, разрешает считывать пинкод. Проверяет пинкод ограниченное количество раз – при успешном вводе открывает замок, по закрытии или при ошибке пароля более 3 раз заказывает расчет и начало пути обратно. По достижении пункта назначения инициирует отправку сообщения со статусом заказа через Communication на Fleet.

![DFD](./pics/dfd_stepan.png?raw=true "Архитектура")

### Алгоритм работы решения

![Sequence diagram](./pics/sd_stepan.png?raw=true "Диаграмма вызовов")

### Описание Сценариев (последовательности выполнения операций), при которых ЦБ нарушаются

Для определения доверенных компонентов, необходимо произвести анализ ризков, сценариев нарушения ЦБ.
  Первые кандидаты на доверенные компоненты - это Security Monitor и Central Control Unit по понятным причинам – один арбитр всей системы, т.е. если он скомпрометирован, то уже никакая цель безопасности не выполняется, а второй – «мозг» системы и при компрометации так же нарушаются цели безопасности – возможность обойти условия.
  
  В теории, используя глобальные переменные, сохраняемые в мониторе безопасности и добавлении в него некоторой усложняющей логики проверок становится возможным использовать в роли доверенного компонента только монитор, однако на мой взгляд более целесообразным представляется исключение механизма навигации из состава Center, поскольку это сильно снижает затраты на проверку. 
  
  Поскольку в реальности офлайн-навигация будет осуществляться на основе данных от gps-модуля, Motion (сколько должно быть пройдено по ресурсу) и через ИНС (вероятно, микромеханические гироскопы), то логичным представляется добавление в Sensors датчиков GPS и ИНС, а в Positioning, соответственно, вычисление реальной координаты и определения пути к указанной цели (в данный момент этого достаточно, а позже цель, например, промежуточную точку объезда препятствия должна будет выдавать соответствующая сущность). 
  
  Таким образом, было принято решение на данный момент передавать напрямую координату требуемой точки от Center в Positioning, который определяет свою координату, направление и расстояние до цели, отдает в Center, который передает их в Motion для осуществления движения и сопутствующего «логгирования». Такая схема позволяет легко масштабироваться при необходимости добавления вышеуказанных датчиков и/или сущности «Навигатор», которая будет строить пути к точке, выданной ей Center, выдавая контрольные точки в Positioning для расчета направления движения.
  
  При таком развитии событий, представляется делать модуль Positioning доверенным, как ответственный за определение реальных координат. Иначе теоретический ущерб заключается в нарушении цели безопасности «прием пинкода только в пункте назначения», что, впрочем, не приводит к нарушению остальных ЦБ, но не позволяет достичь заявленной цели работы. На данный момент это представляется бессмысленным.
  
  Возвращаясь к теме доверенных компонентов - поскольку все сообщение осуществляется через Message Bus при помощи Kafka, требуется, чтобы используемые сторонние компоненты (kafka) так же были надежны – т.е. требуется если не объявлять кафку доверенной, то как минимум отслеживать обновления на появление лишней функциональности.
  
  К счастью, по условиям задачи уже предполагается, что монитор безопасности надёжно защищен от любого взлома и его решениям всегда можно доверять (и видимо обойти его физически нельзя, что стоит уточнить), как и обмен сообщениями через Message Bus, который также надёжно защищен от любого взлома и его решениям всегда можно доверять.
  
  Таким образом получаем три доверенных компоненты: message bus, center и monitor.
  
  Со всеми остальными компонентами ситуация в общем-то аналогичная:
  
  Они все не являются доверенными и потенциально могут быть использованы для нарушения одной из поставленных целей безопасности, выполняя остальные.
  
  Если будет взломан Motioning или Positioning, злоумышленник сможет нарушить поставленную цель безопасности о вручении заказа только в обозначенном в заказе месте, что не позволит получить заказ никому, т.е. является в целом приемлемым риском.
  
  Если будет взломан HMI, то злоумышленник сможет отправлять свой код/узнать верный, но так как запланировано ограниченное количество попыток, считаемое доверенным компонентом, а клиент вводит пароль когда робот уже на месте, то это не принесет пользы злоумышленнику, максимум - не позволит получить заказ никому, т.е. является в целом приемлемым риском.
  
  Проблема могла бы быть если взломан Communication – поскольку в моей реализации заказ один, то получение нового заказа при выполнении старого перезаписала бы требуемый пинкод и адрес и позволила бы злоумышленнику получать чужой заказ. Для противодействия этого была запланирована блокировка политики в Monitor до возвращения робота на базу.
  
  Самой серьезной проблемой представляется взлом замков Sensors, что позволит злоумышленнику их просто открыть. Таким образом, его следует добавить в доверенные компоненты.
  
  Напоминаю, что HMI я считаю встроенным терминалом – таким образом, в робота фактически есть одна внешняя точка доступа – через Communicator, который, к тому же использует Flask, торчит в интернет и так далее. Следовательно – этот компонент следует тщательно досматривать – на политики проверки сообщений от него навесить жесткие ограничения по валидации содержимого: длина, значения, отрубать при возможности, а также уделить особое внимание настройке сетевых параметров так, чтобы связаться с ним можно было только от заранее указанного сервера с авторизацией и т.д. Кроме того, в данном случае можно даже иметь хранилище одноразовых ключей и с сервера высылать подтверждение валидности заказа для проверки в мониторе. 
  
  В результате, была добавлена четвертая доверенная компонента - sensors. 

В результате произведенных размышлений, были сформулированы 6 наиболее вероятных сценариев нарушения ЦБ:
* перезапись "чувствительных" данных при повторном заказе;
* ввод паролей не в пункте назначения;
* перебор паролей;
* получение неверных координат;
* скрытые поля и их использование для code injections;
* взлом Communication как открытой точки доступа в сеть;

Ниже представлены выбранные сценарии компрометации целей безопасности на диаграмме процесса работы:

![Негативные сценарии](./pics/sd_rev_stepan.png?raw=true "Негативные сценарии")

**Отдельная диаграмма для первого негативного сценария - повторного заказа:**
![Негативные сценарии](./pics/sd_rev_1.png?raw=true "")

Здесь нарушаются две ЦБ:
-	обеспечение сохранности груза до момента передачи авторизованному клиенту - поскольку адрес и/или PIN перезаписаны, то авторизован уже злоумышленник.
-	получение задания только от корпоративного сервера - поскольку новый заказ может быть прислан злоумышленником в т.ч. при помощи взлома Communication (см. сценарий #6).

Данная угроза была устранена путем блокировки функции приема заказа при получении до его завершения.

**Отдельная диаграмма для второго негативного сценария - ввод паролей не в пункте назначения:**
![Негативные сценарии](./pics/sd_rev_2.png?raw=true "")

Нарушается ЦБ - осуществлять проверку пинкода только по достижению указанной в заказе точке.

Данная угроза была устранена путем блокировки проверки пинкода до достижения указанной в заказе точки.

**Отдельная диаграмма для третьего негативного сценария - перебора паролей:**
![Негативные сценарии](./pics/sd_rev_3.png?raw=true "")

Нарушаются две ЦБ:
- в случае многократных попыток неавторизованного доступа вернуть груз на склад
- обеспечение сохранности груза до момента передачи авторизованному клиенту - поскольку пароль становится известен злоумышленнику.

Данная угроза была устранена путем реализации счетчика попыток ввода пароля.

**Отдельная диаграмма для четвертого негативного сценария - получения неверных координат:**
![Негативные сценарии](./pics/sd_rev_4.png?raw=true "")

Что инетерсно - ЦБ не нарушается напрямую, но не выполняется бизнес-функция.
В принципе, можно считать, что нарушается ЦБ - осуществлять проверку пинкода только по достижению указанной в заказе точке.

Данная угроза для устранения требует несколько независимых датчиков позиционирования (см. выше) с делегированием доверенному компоненту функции сравнения и выбора корректной среди них.

**Отдельная диаграмма для пятого негативного сценари- code injections и пр. лишних полей в заказе:**
![Негативные сценарии](./pics/sd_rev_5.png?raw=true "")

Неясно, какая ЦБ нарушается, поскольку это зависит от конкретной реализации и эксплуатации уязвимости, поэтому считаем, что нарушается основная бизнес-функция и ЦБ - обеспечение сохранности груза до момента передачи авторизованному клиенту.

Данная угроза была, вероятно, устранена путем реализации строгих политик безопасноти на состав полей нового заказа.

**Отдельная диаграмма для шестого негативного сценария - взлома Communication компонента:**
![Негативные сценарии](./pics/sd_rev_6.png?raw=true "")

Нарушаются две ЦБ:
- получение задания только от корпоративного сервера - поскольку злоумышленник теперь тоже может присылать заказы.
- обеспечение сохранности груза до момента передачи авторизованному клиенту - поскольку злоумышленник так же может узнать пароль.

Данная угроза для устранения требует настроек сетевых параметров, что невозможно обеспечить только на уровне кода, поэтому защита не реализована.


### Указание "доверенных компонент" на архитектурной диаграмме.

В предыдущем параграфе были рассмотрены возможные сценарии нарушения ЦБ, по результатам чего были определены доверенные компоненты - monitor, message bus, central и sensors, по причине того, что шина обмена сообщений и монитор безопасности являются общесистемными, а также мы хотим доверять результатам работы замков хранилища заказа и блока управления, хотя было указано что в теории, блок управления возможно исключить из этого списка при стечении некоторых обстоятельств.

![DFD-TCB](./pics/dfd_stepan_trusted.png?raw=true "Доверенные компоненты")

Теоретически, шину обмена сообщениями можно сделать недоверенным компонентом, но тогда необходимо будет реализовать механим верификации сообщений. Например, можно использовать технологию типа blockchain. 
Цена этого изменения - усложнение обмена сообщениями, дополнительные накладные расходы на верификацию, снижение производительности системы, возможно также возникновение нестабильности и различные ситуации гонок.


### Политики безопасности 


```python {lineNo:true}
ordering = False

def check_operation(id, details):
    global ordering
    authorized = False
    print(f"[info] checking policies for event {id},"\
          f" {details['source']}->{details['deliver_to']}: {details['operation']}")
    src = details['source']
    dst = details['deliver_to']
    operation = details['operation']
    if not ordering:
        if  src == 'communication' and dst == 'central' \
            and operation == 'ordering' and type(details['pincode']) == int \
                and type(details['x']) == int and type(details['y']) == int \
                and abs(details['x']) <= 200 and abs(details['y']) <= 200  and len(details) == 7 :
            authorized = True
            ordering = True
    if src == 'central' and dst == 'communication' \
        and operation == 'confirmation':
        authorized = True    
    if src == 'central' and dst == 'positioning' \
        and operation == 'count_direction':
        authorized = True    
    if src == 'positioning' and dst == 'central' \
        and operation == 'count_direction':
        authorized = True    
    if src == 'central' and dst == 'motion' \
        and operation == 'motion_start':
        authorized = True    
    if src == 'motion' and dst == 'positioning' \
        and operation == 'motion_start':
        #and details['verified'] is True:
        authorized = True    
    if src == 'motion' and dst == 'positioning' \
        and operation == 'stop':
        authorized = True    
    if src == 'positioning' and dst == 'central' \
        and operation == 'stop':
        authorized = True
    if src == 'hmi' and dst == 'central' \
        and operation == 'pincoding':
        authorized = True    
    if src == 'central' and dst == 'sensors' \
        and operation == 'lock_opening':
        authorized = True
    if  src == 'sensors' and dst == 'central'\
        and operation == 'lock_closing':
        authorized = True
    if  src == 'central' and dst == 'communication'\
        and operation == 'ready':
        authorized = True
        ordering = False
    return authorized
```

## Запуск приложения и тестов

### Запуск приложения

см. [инструкцию по запуску](../../README.md)

### Запуск тестов

_Предполагается, что в ходе подготовки рабочего места все системные пакеты были установлены._

Запуск примера: открыть окно терминала в Visual Studio code, в папке secure-update с исходным кодом выполнить 

**make run**
или **docker-compose up -d**

Примечание: сервисам требуется некоторое время для начала обработки входящих сообщений от kafka, поэтому перед переходом к тестам следует сделать паузу 1-2 минуты

запуск тестов (на данный момент отсутсвуют):
**make test**
или **pytest**
