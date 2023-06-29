# Домашнее задание 1

Цель проекта - разработать за полгода конкурентоспособную антифрод-систему.  Основная часть системы должна размещаться на внешних ресурсах. Система должна выдерживать высокие нагрузки. Также необходимо обеспечить конфиденциальность данных. За первые три месяца предоставить MVP.

Так как было определено, что у конкурентов максимум 2% мошеннических транзакций приводят к потере средств, то будем использовать метрику *False Negative Rate*:
$$FNR = \frac{FN}{FN+TP} * 100\% $$, где *FN* - число мошеннических транзакций, приведших к потере средств; *TP* - число распознаных мошеннических транзакций моделью. Данная метрика должна быть меньше 2%.

Для контроля недовольства клиентов – *False Positive Rate*. Доля ложных блокировок транзакций честного пользователя от общего числа транзакций.


$$FPR = \frac{FP}{FP+TN} * 100\%$$, где *FP* - число ложных срабатываний; *TN* - число обычных транзакций, которые распознаны как **не**мошеннические. Данная метрика должна быть меньше 5%.

Бизнес-метрика -- ущерб от мошеннических транзакций за месяц. Не должна превышать 500 тыс. рублей.



