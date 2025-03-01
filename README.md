# Raft Consensus
**Авторы**: Владислав Ненов P34082, Михаил Передрий P32102

## 1. Требования к разработанному ПО
### Функциональные требования:  
Реализация алгоритма Raft для достижения консенсуса в распределенной системе.
  
1. Составляющие алгоритма:  
   - Выборы лидера  
   - Репликация логов
   - Обработка крайних случаев (временная недоступность узлов системы, перебои в работе и тд)    
2. Внешний API:  
   - Добавления узлов в кластер.  
   - Добавление данных в распределенный лог.  
   - Получение снэпшотов лога.  

### Нефункциональные требования:  
- Отказоустойчивость при потере сообщений.  
- Гарантия согласованности данных.  
- Минимальная задержка при выборах лидера (таймауты ~150-300 мс).  

### Описание алгоритма:  
Полное описание алгоритма, согласно которому проводилась имплементация можно найти по [ссылкe](https://raft.github.io/raft.pdf).

Алгоритм Raft разделен на 2 ключевых этапа:  
1. **Выборы лидера**:  
   - Узлы переходят в состояние *кандидата* при истечении таймера
   - Узлы-кандидаты начинают голосование за "лидерство" в кластере
   - Один из кандидатов собирает голоса большинства и становится лидером  
2. **Репликация логов**:  
   - Лидер рассылает записи последовательно всем узлам. 
   - После репликации на большинстве узлов, запись считается подтвержденной 
   - Лидер отслеживает согласованность данных, получая ответы на *heartbeat* 
   - Если выявлена неконсистентность, лидер заменяет элементы лога последователя, пока не добьется полного соответствия с локальным логом.

При этом каждый узел системы может находиться только в одном из 3-х состояний: 
-  последователь `follower`
-  кандидат `candidate`
-  лидер `leader` 

## 2. Реализация  
### Основные модули:  
- **raft.erl**: алгоритм *raft* по шаблону `gen_fsm`.  
- **log.erl**: Управление реплицируемым логом данных.  
- **snapshot_providers/***: провайдеры, ответственные за создание снэпшотов лога.
- **tests/***: Несколько тестов на простейшие сценарии.  

## 3. Ввод/вывод программы  

### 1. Инициализация узла  
  ```erlang
  {ok, Node1} = raft_rpc:start_node(node1, snapshot_server).
  ```  

### 2. **Добавление узла в кластер**  
```erlang
raft_rpc:add_node(node1, node2).
```  

### 3. **Перевести узел в начальное состояние**  
  ```erlang
raft_rpc:start_follower(node1).
```  

### 4. **Отправка данных**  
```erlang
raft_rpc:send_data(node1, "Hello Raft"). % Добавить данные в лог
```  

### 5. **Создание снэпшота**  
```erlang
make_snapshot(NodeRef) -> Snapshot
make_snapshot(NodeRef, Provider) -> Snapshot % С кастомным провайдером
```  

```erlang
Snapshot = raft_rpc:make_snapshot(node1, custom_provider).
```  

### 6. **Инициализация кластера**  
```erlang
init_cluster(Nodes) -> ok
init_cluster(Nodes, SnapshotProvider) -> ok
```  

1. Запускает все узлы из списка `Nodes`.  
2. Регистрирует их в одну сеть.  
3. Переводит все узлы в начальное состояние (Follower).  


```erlang
raft_rpc:init_cluster([node1, node2, node3]).
```  

### Пример вывода при работе:  
```erlang
%% Запуск кластера из 4 узлов
> raft_rpc:init_cluster([n1, n2, n3, n4], concat_str_provider).

%% Отправка данных
> raft_rpc:send_data(n1, "str1").
> raft_rpc:send_data(n3, "str3").

%% Создание снэпшота
> raft_rpc:make_snapshot(n1).
{ok,"str1str3"}
```

Помимо этого подробнее изучить переходы между состояними и логику работы программы можно заглянув в `logs/debug.log`.


## 4. Выводы  
В процессе работы мы изучили спецификацию алгоритма консенсуса *Raft* и имплементировали его с использованием шаблона `gen_fsm` на языке программирования Erlang.
