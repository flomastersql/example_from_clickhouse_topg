<?php


require __DIR__ . '/vendor/autoload.php';

$nameAction = "Top50perrsMigrator";
$timeStartUnix = time();

//$timeTripleWatching = date('Y-m-d H:i:s', strtotime('2019-07-17 10:00:00')+1000);

//    ->format('Y-m-d H:i:s');

// <editor-fold defaultstate="collapsed" desc="Действие 1. Подключение к серверам">
//к  БД Клика Интеры Пирс
$config = new \ClickhouseClient\Client\Config(
// basic connection information
    ['host' => '', 'port' => '8123', 'protocol' => 'http']
);
$config->setUser('');
$config->setPassword('');
$config->change('database', '');

$dbCl = new \ClickhouseClient\Client\Client($config);

//к БД микросервиса qmsDb
$dbPg = @pg_connect("host= port=5434 dbname=
                 user= password=");

$dbPgStalker = @pg_connect("host= port= dbname=
                 user= password=");

echo "connected to bases - ok";

// </editor-fold>

// <editor-fold defaultstate="collapsed" desc="Действие 2. Получение максимального времени с загруженых ранее данных">
$sqlMaxTimeInBufer = "
            select max(time_watch) from peers.film_watch_from_tracking_events_untyped_row_buffer
            ";

$resMaxTimeInBufer = pg_query($dbPg, $sqlMaxTimeInBufer);
$MaxTimeInBufer = pg_fetch_result($resMaxTimeInBufer, 0, 0);

//$MaxTimeInBufer = '2019-08-20 05:50:00';

echo "\nmax time of previous downloading recieved: $MaxTimeInBufer";

// </editor-fold>

// <editor-fold defaultstate="collapsed" desc="Действие 3. Забор данных peers из ClickHouse на уровень скрипта ">


$response = @$dbCl->query("
    select datetime, ip, time, tth, t
     from peers.tracking_events_untyped pteu
     where t in ('film/watch','film/start')
       and pteu.datetime >= '$MaxTimeInBufer'
        and pteu.time <> '0'
        and pteu.tth <> ''
        order by pteu.datetime
");

$data = $response->getContent()["data"];

$response = @$dbCl->query("
select toDateTime(max(datetime), 'Asia/Novosibirsk') m from
 peers.tracking_events_untyped pteu where
pteu.datetime >= '$MaxTimeInBufer'
");

$maxDateTimeInClickHouse = $response->getContent()["data"][0]["m"];

echo "\n" . count($data) . " rows of views recieved from clickhouse";
echo "\nmax time from clickhouse recieved: $maxDateTimeInClickHouse";

$grafana_input = false;
$hourOfMaxDateTimeInClickHouse = substr($maxDateTimeInClickHouse, 11, 2);
$hourOfMaxTimeInBufer = substr($MaxTimeInBufer, 11, 2);

//если начало часа
if ($hourOfMaxDateTimeInClickHouse <> $hourOfMaxTimeInBufer) {
    $grafana_input = true;

    take_from_pg__put_to_pg(
        $dbPgStalker
        , $dbPg
        , "
drop table if exists peers.stalker_films_nsi_buffer;

create table peers.stalker_films_nsi_buffer
(
    fid bigint not null,
    size bigint,
    fname varchar,
    tth varchar,
    mag_id bigint not null,
    title varchar
); "
        , "
    select t.fid
      , t.size
      , t.fname
      , t.tth
      , t.mag_id
      , replace(f.title, '''', '')
    from cn.film_mag t
          left join cn.films f on t.fid = f.id
    union all
    select t.fid
      , t.size
      , t.fname
      , t.tth
      , t.mag_id
      , replace(f.title, '''', '')
    from cn.web_mag t
          left join cn.films f on t.fid = f.id
    where t.tth not in (select tth from cn.film_mag) "
        , "peers.stalker_films_nsi_buffer"
        , 10000
    );
}


// </editor-fold>

// <editor-fold defaultstate="collapsed" desc="Действие 4. Берем очередной евент из полученных данных, проверяем если ли в буфере ">

$rowsIsInBuf = 0;
$rowsInputInBuf = 0;
$count_data = count($data);

for ($i = 0; $i < $count_data; $i++) {
    $time_watch = $data[$i]['datetime'];
    $ip = $data[$i]['ip'];
    $dur = $data[$i]['time'];
    $tth = $data[$i]['tth'];
    $t = $data[$i]['t'];

    //вставляем в буфер
    $sqlIsInBuffer = "
            select ip from peers.film_watch_from_tracking_events_untyped_row_buffer
            where time_watch = '$time_watch' and ip = '$ip' and dur = '$dur' and t = '$t'
            ";
    $resIsInBuffer = pg_query($dbPg, $sqlIsInBuffer);
    $IsInBuffer = pg_fetch_all($resIsInBuffer);
    //если прям такого значения нет
    if (!$IsInBuffer) {
        //заополняем буфер
        //ошибка по умолчани - без ошибки
        $error = 0;
        //</editor-fold>

// <editor-fold defaultstate="collapsed" desc="Действие 5. Обработка события начала фильма">
        if ($t == "film/start") {
            //проверяем предыдущее событие ip на начало
            $isPrevEventOfIpStrartWatching = pg_fetch_all(pg_query($dbPg, "
            select tth, time_watch from peers.film_watch_from_tracking_events_untyped_row_buffer where
                time_watch = (
                        select max(time_watch) from peers.film_watch_from_tracking_events_untyped_row_buffer where ip = '$ip'
                    ) and t = 'film/start' and ip = '$ip'
            "));
            //если предыдущее событие по ip тоже началло, то необходимо сначала закрыть предыдущий, преждем чем регистрировать начало просмотра текущего
            if ($isPrevEventOfIpStrartWatching) {
                //берем длину видео из соображения 1Гб=1час
                $sqlDurMovieBySizeStalker = "
                select
                   case when fname like '%full%hd%' then round(3.6*size/1024/1024/6)
                       else round(3.6*size/1024/1024)
                   end
                from magnet
                where tth = '" . $isPrevEventOfIpStrartWatching[0]["tth"] . "'";
                $durMovieBySizeStalker = (int)pg_fetch_result(pg_query($dbPgStalker, $sqlDurMovieBySizeStalker), 0, 0);
                // чистим данные рейтинга ТОГО открытого просмотра
                $deletingOpenPrevWatching = "
                    delete from peers.film_watch_rating where
                    ip = '$ip'
                    and tth = '" . $isPrevEventOfIpStrartWatching[0]["tth"] . "'
                    and watching_now;
                ";
                pg_query($dbPg, $deletingOpenPrevWatching);

                $durFromStartPrevMovieToStartThisMovie = strtotime($time_watch) - strtotime($isPrevEventOfIpStrartWatching[0]["time_watch"]);

                // вставляем его заново в рейтинг уже закрытым
                if ($durFromStartPrevMovieToStartThisMovie > $durMovieBySizeStalker * 3) {
                    // либо эквивалентно длине трех просмотров видео, если расстояние между событиями больше чем три просмотра ТОГО фильма
                    $error = 8; // начало следующего фильма без конца предыдущего. запись в рейтинг от старта ТОГО эквивалентно тройной длине фильма из расчета 1Гб = 1Час
                    echo "\nerror = 8 : manual_close_prev_watch triple during $ip " . $isPrevEventOfIpStrartWatching[0]["tth"];
                    $timeTripleWatching = date('Y-m-d H:i:s', strtotime($isPrevEventOfIpStrartWatching[0]["time_watch"]) + $durMovieBySizeStalker * 3);
                    $closePrevWatchArray = getHoursArrayWithDuringFromTimeStartAndEnd($isPrevEventOfIpStrartWatching[0]["time_watch"], $timeTripleWatching);
                } else { // либо эквивалентну расстоянию от старта ТОГО фильма до старта ЭТОГО
                    $error = 1; // начало следующего фильма без конца предыдущего. запись в рейтинг от старта ТОГО фильма до старта ЭТОГО
                    // ("error = 1" ищется в схеме Алгоритм работы с рейтингом фильмов: https://drive.google.com/drive/folders/1nfLmaz0V2wkZnwdRkxmeK-QLZ39G9JNG
                    echo "\nerror = 1 : manual_close_prev_watch during from start prev to start this $ip " . $isPrevEventOfIpStrartWatching[0]["tth"];
                    $closePrevWatchArray = getHoursArrayWithDuringFromTimeStartAndEnd($isPrevEventOfIpStrartWatching[0]["time_watch"], $time_watch);
                }

                foreach ($closePrevWatchArray as $curHour) {
                    pg_query($dbPg, "
                    insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '" . $isPrevEventOfIpStrartWatching[0]["tth"] . "', $error)
                    ");
                }

            }
            //начало регистрации просмотра текущего фильма делаем независимо от наличия фиксации предыдущей ошибки
            //..
            //получили почасовой массив данных просмотра от времени начала конца, до максимального времени в ClickHouse
            $startWatchingArray = getHoursArrayWithDuringFromTimeStartAndEnd($time_watch, $maxDateTimeInClickHouse);
            //вставили его в таблицу рейтинга, с обязательным условием что просмотр открыт (предпоследний флаг = true
            echo "\nbegin_watch $ip $tth";
            foreach ($startWatchingArray as $curHour) {
                pg_query($dbPg, "
                insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", true, '$tth', $error)
                ");
            }


        } // окончание обработки события film_start
        else { // если событие - film/watch - конец просмотра фильма
//</editor-fold>
// <editor-fold defaultstate="collapsed" desc="Действие 6. Обработка события конца фильма">
            $isPrevEventOfIp = pg_fetch_all(pg_query($dbPg, "
                select t, tth, time_watch, dur from peers.film_watch_from_tracking_events_untyped_row_buffer where
                time_watch = (
                        select max(time_watch) from peers.film_watch_from_tracking_events_untyped_row_buffer where ip = '$ip'
                    ) and ip = '$ip'
            "));
            // есть предыдущее событие по данному ip?
            if ($isPrevEventOfIp) {
                // это событие начало фильма?
                if ($isPrevEventOfIp[0]["t"] == "film/start") {
                    // это событие начало ЭТОГО фильма?
                    if ($isPrevEventOfIp[0]["tth"] == $tth) {
                        //удаляем из рейтинга текущий открытый просмотр
                        $deletingOpenThisWatching = "
                            delete from peers.film_watch_rating where
                            ip = '$ip'
                            and tth = '$tth'
                            and watching_now;
                        ";
                        pg_query($dbPg, $deletingOpenThisWatching);

                        $normFinishWatchingArray = getHoursArrayWithDuringFromTimeStartAndEnd($isPrevEventOfIp[0]["time_watch"], $time_watch);
                        // вставили его в таблицу рейтинга, с обязательным условием что просмотр открыт (предпоследний флаг = true
                        echo "\nnorm finish watching $ip $tth";
                        foreach ($normFinishWatchingArray as $curHour) {
                            pg_query($dbPg, "
                            insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                            ");
                        }
                        //конец фильма сценарий 1
                    } else { // нет, начало ТОГО
                        $time10hAgo = date('Y-m-d H:i:s', strtotime($maxDateTimeInClickHouse) - 36000);

                        $sqlIsStartWatchingThisVideoLast10h = "
                        select t, tth, time_watch from peers.film_watch_from_tracking_events_untyped_row_buffer
                        where ip = '$ip'
                        and tth = '$tth'
                        and time_watch = (select max(time_watch) from peers.film_watch_from_tracking_events_untyped_row_buffer where ip = '$ip' and tth = '$tth')
                        and time_watch > '$time10hAgo' and t = 'film/start'
                        ";

                        $isStartWatchingThisVideoLast10h = pg_fetch_all(pg_query($dbPg, $sqlIsStartWatchingThisVideoLast10h));
                        if ($isStartWatchingThisVideoLast10h) { // в буфере есть начало этого фильма без конца за последние 10 часов
                            echo "\nerror = 2. Parallel watching video from one ip";
                            $time10hPlusAgo = date('Y-m-d H:i:s', strtotime($maxDateTimeInClickHouse) - 40000);
                            $error = 2;
                            //удалить в рейтинге предыдущее событие этого фильма ,которое прервалось началом другого фильма
                            pg_query($dbPg,
                                "
                            delete from peers.film_watch_rating where  ip = '$ip'
                            and tth = '$tth'
                            and hour_watch > '$time10hPlusAgo' and error in (1, 8)
                            ");
                            //получаем массив для вставки
                            $parallelFinishWatchingArray = getHoursArrayWithDuringFromTimeStartAndEnd($isStartWatchingThisVideoLast10h[0]['time_watch'], $time_watch);
                            //вставляем в рейтинг обновленное событие этого фильма
                            foreach ($parallelFinishWatchingArray as $curHour) {
                                pg_query($dbPg, "
                                    insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                ");
                            }
                            // конец фильма сценарий 2
                        } else { //
                            echo "\nerror = 4. Parallel watching video from one ip. Fix by end film (lost event start film)";
                            $error = 4;
                            // конец фильма сценарий 3
                            $timeStartWatchingByTimeEndAndDurData = date('Y-m-d H:i:s', strtotime($time_watch) - (int)$dur);
                            $closeWatchingArrayWithoutStart = getHoursArrayWithDuringFromTimeStartAndEnd($timeStartWatchingByTimeEndAndDurData, $time_watch);
                            foreach ($closeWatchingArrayWithoutStart as $curHour) {
                                pg_query($dbPg, "
                                insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                ");
                            }
                        }
                    }
                } else { // нет - время конца

                    if ($isPrevEventOfIp[0]["tth"] == $tth) { // конца ЭТОГО фильма?
                        //время конца просмотра ТОГО фильма раньше, чем вычисленное по времени события и длительности времени начала ЭТОГО ? (это про один и тот же фильм)
                        if (strtotime($isPrevEventOfIp[0]["time_watch"]) < (strtotime($time_watch) - (int)$dur)) {
                            echo "\nerror = 12. avaliable two events ending one movie in a row. losted event of start watching";
                            $error = 12;
                            $timeStartWatchingByTimeEndAndDurData = date('Y-m-d H:i:s', strtotime($time_watch) - (int)$dur);
                            $closeWatchingArrayWithoutStart = getHoursArrayWithDuringFromTimeStartAndEnd($timeStartWatchingByTimeEndAndDurData, $time_watch);
                            foreach ($closeWatchingArrayWithoutStart as $curHour) {
                                pg_query($dbPg, "
                                insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                ");
                            }
                        } else {
                            echo "\nerror = 10. unvaliable two events ending one movie in a row";
                            $error = 10;
                        }


                    } else { //нет - конца другого фильма
                        $time10hAgo = date('Y-m-d H:i:s', strtotime($maxDateTimeInClickHouse) - 36000);
                        $sqlIsStartWatchingThisVideoLast10h = "
                        select t, tth, time_watch from peers.film_watch_from_tracking_events_untyped_row_buffer
                        where ip = '$ip'
                        and tth = '$tth'
                        and time_watch = (select max(time_watch) from peers.film_watch_from_tracking_events_untyped_row_buffer where ip = '$ip' and tth = '$tth')
                        and time_watch > '$time10hAgo' and t = 'film/start'
                        ";
                        $isStartWatchingThisVideoLast10h = pg_fetch_all(pg_query($dbPg, $sqlIsStartWatchingThisVideoLast10h));

                        if ($isStartWatchingThisVideoLast10h) {
                            $error = 5;
                            $isThisVideoWatchingNow = pg_fetch_all(pg_query($dbPg, "
                            select watching_now from peers.film_watch_rating where tth = '$tth'
                            and hour_watch = (select max(hour_watch) from peers.film_watch_rating where tth = '$tth')
                            and ip = '$ip' and watching_now
                                        "));
                            if ($isThisVideoWatchingNow) { //если фильм числится как смотрящийся в настоящий момент
                                //удаляем из рейтинга текущий открытый просмотр
                                $deletingOpenThisWatching = "
                                delete from peers.film_watch_rating where
                                ip = '$ip'
                                and tth = '$tth'
                                and watching_now;
                            ";
                                pg_query($dbPg, $deletingOpenThisWatching);


                            } else { // если не числтся, то был прикрыт за событием 1 или 8 началом другого фильма
                                $time12hPlusAgo = date('Y-m-d H:i:s', strtotime($maxDateTimeInClickHouse) - 40000);
                                //удалить в рейтинге предыдущее событие этого фильма , которое прервалось началом другого фильма
                                $sqlDeleteStopedFilm = "
                                    delete from peers.film_watch_rating where  ip = '$ip'
                                    and tth = '$tth'
                                    and hour_watch > '$time12hPlusAgo' and error in (1, 8)
                                ";

                                pg_query($dbPg, $sqlDeleteStopedFilm);
                            }
                            $finishWatchingArrayVer4 = getHoursArrayWithDuringFromTimeStartAndEnd($isStartWatchingThisVideoLast10h[0]["time_watch"], $time_watch);
                            // вставили его в таблицу рейтинга, с обязательным условием что просмотр открыт (предпоследний флаг = true
                            echo "\nnorm finish watching $ip $tth";
                            foreach ($finishWatchingArrayVer4 as $curHour) {
                                pg_query($dbPg, "
                                    insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                    ");
                            }
                            // конец фильма сценарий 4
                        } else {
                            //время конца просмотра ТОГО фильма раньше, чем вычисленное по времени события и длительности времени начала ЭТОГО ?
                            if (strtotime($isPrevEventOfIp[0]["time_watch"]) < (strtotime($time_watch) - (int)$dur)) {
                                $error = 6;
                                echo "\nerror = 6. lost start event of this film";
                                // конец фильма сценари 3
                                $timeStartWatchingByTimeEndAndDurData = date('Y-m-d H:i:s', strtotime($time_watch) - (int)$dur);
                                $closeWatchingArrayWithoutStart = getHoursArrayWithDuringFromTimeStartAndEnd($timeStartWatchingByTimeEndAndDurData, $time_watch);
                                foreach ($closeWatchingArrayWithoutStart as $curHour) {
                                    pg_query($dbPg, "
                                insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                ");
                                }
                            } else {
                                if ((int)$dur == (int)$isPrevEventOfIp[0]["dur"]) {
                                    $error = 7;
                                    echo "\nerror = 7. unvaliable watching film";
                                } else {
                                    $error = 11;
                                    echo "\nerror = 11. lost start event of this film";
                                    // конец фильма сценари 3
                                    $timeStartWatchingByTimeEndAndDurData = date('Y-m-d H:i:s', strtotime($time_watch) - (int)$dur);
                                    $closeWatchingArrayWithoutStart = getHoursArrayWithDuringFromTimeStartAndEnd($timeStartWatchingByTimeEndAndDurData, $time_watch);
                                    foreach ($closeWatchingArrayWithoutStart as $curHour) {
                                        pg_query($dbPg, "
                                        insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                        ");
                                    }
                                }
                            }
                        }
                    }
                }

            } else { // нет предыдущего события по данному ip?
                echo "\nerror = 9. fst event of ip - finish watching movie";
                $error = 9;
                // конец фильма сценари 3
                $timeStartWatchingByTimeEndAndDurData = date('Y-m-d H:i:s', strtotime($time_watch) - (int)$dur);
                $closeWatchingArrayWithoutStart = getHoursArrayWithDuringFromTimeStartAndEnd($timeStartWatchingByTimeEndAndDurData, $time_watch);
                foreach ($closeWatchingArrayWithoutStart as $curHour) {
                    pg_query($dbPg, "
                                insert into peers.film_watch_rating values ('" . $curHour[0] . "', '$ip', " . $curHour[1] . ", false, '$tth', $error)
                                ");
                }

            }

        } // конец обработки события film/watch
//</editor-fold>


        $sqlInsInBuffer = "
                insert into peers.film_watch_from_tracking_events_untyped_row_buffer (time_watch, ip, dur, tth, t, error) 
                values ('$time_watch', '$ip', '$dur', '$tth', '$t', $error)
        ";
        pg_query($dbPg, $sqlInsInBuffer);
        $rowsInputInBuf++;
    } else { //пропускаем строку если она уже есть в буфере (на времени стыка предыдущей и прошлой загрузки)
        $rowsIsInBuf++;
        //НЕ ЗАБУДЬ ИСКЛЮЧИТЬ ИЗ МАССИВА!
        unset($data[$i]);
    }
}

echo "\nпоместил $rowsInputInBuf строк в буфер";
echo "\nпропустил $rowsIsInBuf строк, которые в буфере уже есть";

//</editor-fold>


// <editor-fold defaultstate="collapsed" desc="Действие 100. Загрузка данных в графану">

//если в буфер поступили данные за новый час, то закинуть данные
if ($grafana_input) {
    $hourOfMaxTimeClick = substr($maxDateTimeInClickHouse, 0, 13) . ":00:00";
    $prevHourOfMaxTimeClick = getPrevHour($hourOfMaxTimeClick);


    $dataForGrafana = pg_fetch_all(pg_query($dbPg, "
            select err_dict.descr, err.vol
        from (
         select error, count(error) vol
         from peers.film_watch_from_tracking_events_untyped_row_buffer
         where time_watch > '$prevHourOfMaxTimeClick'
           and time_watch <= '$hourOfMaxTimeClick'
           and error <> 0
         group by error
     ) err left join peers.film_watch_error_dict err_dict on err.error = err_dict.id
            "));

    $countEv = pg_fetch_all(pg_query($dbPg, "
             select count(*)
             from peers.film_watch_from_tracking_events_untyped_row_buffer
             where time_watch > '$prevHourOfMaxTimeClick'
             and time_watch <= '$hourOfMaxTimeClick' 
             "));

    foreach ($dataForGrafana as $err) {
        $descr = $err["descr"];
        $per = round(100 * (float)$err["vol"] / (int)$countEv[0]["count"], 2);
        pg_query($dbPg,
            "
            insert into grafana.peers_film_watch_error_trends values (
            '$prevHourOfMaxTimeClick',
            '$descr',
            '$per'
            )
            "
        );
    }
}


// </editor-fold>


echo "\n\nИнкремент количества выполнения.";
$sqlIncrStatus = "
            update _control_actions_with_hours_report set actual_value = actual_value + 1 where name_action = '$nameAction'
            ";
$resultIncrStatus = pg_query($dbPg, $sqlIncrStatus);

echo "\n\nФиксация времени выполнения";
//Берем предыдущее максимальное время выполнения
$sqlTakePreviousMaxTimeExec = "
            select max_exec_time from _control_actions_with_hours_report where name_action = '$nameAction'
            ";
$resultTakePreviousMaxTimeExec = pg_query($dbPg, $sqlTakePreviousMaxTimeExec);
$PreviousMaxTimeExec = (int)pg_fetch_result($resultTakePreviousMaxTimeExec, 0, 0);

//Если текущее время выполнения больше предыдущего максимельного, то обновляем максимальное время
$CurTimeExec = time() - $timeStartUnix;
if ($CurTimeExec > $PreviousMaxTimeExec) {
    $sqlUpdateMaxExecTime = "
            update _control_actions_with_hours_report 
            set max_exec_time = $CurTimeExec
            , max_exec_time_when = now() AT TIME ZONE 'Asia/Vladivostok'            
            where name_action = '$nameAction'
            ";
    $resultUpdateMaxExecTime = pg_query($dbPg, $sqlUpdateMaxExecTime);
}


// </editor-fold>

//функци получает время начала и время конца в строках и раскладывает событие по часам с длительностью в сек
function getHoursArrayWithDuringFromTimeStartAndEnd($timeStartString, $timeEndString)
{
    //получаем общуую длительность просмотра в сек
    $dur = strtotime($timeEndString) - strtotime($timeStartString);
    //получаем строку времени первого часа с конца
    $hour = substr($timeEndString, 0, 13) . ":00:00";
    //получаем время в сек до начала первого часа с конца
    $timeToBeginHour = strtotime($timeEndString) - strtotime($hour);

    //объявляем выходной массив
    $hourArrayOut = [];
    //до тех пор пока оставшаяся длительность просмотра больше, чем код-во секунд до очередного часа
    while ($dur > $timeToBeginHour) {
        //если сюда зашли, то сразу кладём в массив время до начала часа
        array_push($hourArrayOut, [$hour, $timeToBeginHour]);
        //отрезаем от длительности просмотра, то что уже положили в массив в пред.строке
        $dur = $dur - $timeToBeginHour;
        //средствами DateTime получаем предыдущий час в строке
        $prevHour = DateTime::createFromFormat('Y-m-d H:i:s', $hour);
        $prevHour = $prevHour->sub(new DateInterval('PT1H'));
        $hour = $prevHour->format('Y-m-d H:i:s');
        //выставляем время в сек до начала след с конца часа равное кол-ву секунд в часе
        $timeToBeginHour = 3600;
        //если dur все ещё больше часа, то цикл пойдёт его кромсать снова, отступая на час
    }
    //повыходу из цикла (или сразу) вставляем заключительный с конца (первый с начала) отрезок просмотра
    array_push($hourArrayOut, [$hour, $dur]);

    return $hourArrayOut;
}


function getPrevHour($time)
{

    $ymd = DateTime::createFromFormat('Y-m-d H:i:s', $time);
    $ymd = $ymd->sub(new DateInterval('PT1H'));
    $out = $ymd->format('Y-m-d H:i:s');

    return $out;
}

function getNextHour($time)
{

    $ymd = DateTime::createFromFormat('Y-m-d H:i:s', $time);
    $ymd = $ymd->add(new DateInterval('PT1H'));
    $out = $ymd->format('Y-m-d H:i:s');

    return $out;
}


function take_from_pg__put_to_pg($db_source, $db_dest, $sql_clear, $sql_ask_from_source, $copy_target_table_buf, $int_conssole_status_show = 1000)
{
    $fst_time = true;
    //чистим старую таблицу через удаление
    $clear_res = pg_query($db_dest, $sql_clear);
    if (!$clear_res) {
        throw new \Exception($sql_clear);
    }
    //просим и получаем данные с источника
    $result = pg_query($db_source, $sql_ask_from_source);
    if (!$result) {
        throw new \Exception($sql_ask_from_source);
    }
    $data = pg_fetch_all($result);
    //кол-во строк массива для инфо-вывода
    $cnt_row_of_data = count($data);
    //команда для накидывания в получателя по 100 инсертов
    $ins_com_for_100_rows = "";
    //счетчик для выполнения команды
    $ins_sch = 0;
    //счетчики для информирования
    $info_sch = 0;
    $info_sch_incr = 0;
    //идем по строкам
    foreach ($data as $row) {
        $one_row_ins_com = "insert into " . $copy_target_table_buf . " values(";
        $data_of_one_row_ins_com = "";
        //идем по каждому аргументу строки
        foreach ($row as $column) {
            if ($column) {
                $data_of_cur_col = "'" . $column . "'";
            } elseif ($column === "") {
                $data_of_cur_col = "''";
            } elseif ($column === "0") {
                $data_of_cur_col = "'0'";
            } else {
                $data_of_cur_col = "null";
            }
            //собираем тело данных одного инсерта
            if ($data_of_one_row_ins_com) {
                $data_of_one_row_ins_com .= ", ";
            }
            $data_of_one_row_ins_com .= $data_of_cur_col;
        }
        //дособираем инсерт для одной строки
        $one_row_ins_com .= $data_of_one_row_ins_com . ")";
        //собирем 100 инсертов для передачи
        $ins_com_for_100_rows .= $one_row_ins_com . ";";
        //плюсуем все счетчики
        $ins_sch++;
        $info_sch++;
        $info_sch_incr++;
        //инсертим в сервер куда мигрируем как собрали 100 инсертов
        if ($ins_sch >= 100) {
            $ins_res = pg_query($db_dest, $ins_com_for_100_rows);
            if (!$ins_res) {
                throw new \Exception($ins_res);
            }
            $ins_sch = 0;
            $ins_com_for_100_rows = "";
        }
        //информируем в консоль как собрали 1000 строк
        if ($info_sch >= $int_conssole_status_show) {
            if ($fst_time) {
                $fst_time = false;
                echo "\n";
            }
            $info_sch = 0;
            echo "\nLoaded $info_sch_incr from $cnt_row_of_data table's rows $copy_target_table_buf on PG level";
        }
    }
    //выйдя из цикла строк в 99 из 100 случаев в команде останутся данные - поэтому докидываем.
    if ($ins_com_for_100_rows) {
        $ins_res = pg_query($db_dest, $ins_com_for_100_rows);
        if (!$ins_res) {
            throw new \Exception($ins_res);
        }
    }
    echo "\nLoaded $info_sch_incr from $cnt_row_of_data table's rows $copy_target_table_buf on PG level";
}
