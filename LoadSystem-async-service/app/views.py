from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import time
import requests
import json
from concurrent import futures
import random
from datetime import datetime
import sys
import logging

# Настройка логирования
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# URL основного Go сервиса для отправки результатов
GO_SERVICE_URL = "http://localhost:8080/api/total_loads/updating"
AUTH_TOKEN = "loadsys12"  # Псевдо-токен на 8 байт

executor = futures.ThreadPoolExecutor(max_workers=1)


def calculate_total_load(data):
    """
    Формула: Qtotal = Σпост + 0.7 * Σврем
    """
    session_id = data.get('id')
    loads = data.get('loads', [])
    
    start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    logger.info(f"[ASYNC SERVICE] Получен запрос на расчет для LoadSession ID: {session_id}")
    logger.info(f"[ASYNC SERVICE] Начало обработки заявки #{session_id}")
    sys.stdout.flush()
    
    # Задержка 5-10 секунд
    delay = random.uniform(5, 10)
    delay_start_time = time.time()
    delay_start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    logger.info(f"[ASYNC SERVICE] Начало задержки для LoadSession ID: {session_id}, задержка: {delay:.2f} секунд")
    sys.stdout.flush()
    
    time.sleep(delay)
    
    delay_end_time = time.time()
    delay_end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    actual_delay = delay_end_time - delay_start_time
    
    logger.info(f"[ASYNC SERVICE] Задержка завершена для LoadSession ID: {session_id}, прошло: {actual_delay:.2f} секунд")
    sys.stdout.flush()
    
    psi = 0.7  # коэффициент сочетания временных нагрузок
    permanent_loads_sum = 0.0
    temporary_loads_sum = 0.0
    
    # Рассчитываем нагрузку для каждой нагрузки в сессии
    for load in loads:
        area = load.get('area')
        if area is None or area <= 0:
            continue  # пропускаем нагрузки без площади
        
        normative = float(load.get('normative', 0))
        reliability_coeff = float(load.get('reliability_coefficient', 0))
        category = load.get('load_category', '')
        
        load_value = normative * reliability_coeff * float(area)
        
        if category == "Постоянная":
            permanent_loads_sum += load_value
        elif category == "Временная":
            temporary_loads_sum += load_value
    
    # Формула: Qtotal = Σпост + ψ * Σврем
    total_load = permanent_loads_sum + psi * temporary_loads_sum
    
    calc_end_time = time.time()
    calc_end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    calc_duration = calc_end_time - start_time
    
    logger.info(f"[ASYNC SERVICE] Расчет завершен для LoadSession ID: {session_id}, total_load: {total_load:.2f}, время расчета: {calc_duration:.2f} секунд")
    sys.stdout.flush()
    
    result_payload = {
        "id": session_id,
        "total_load": total_load
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": AUTH_TOKEN
    }
    
    try:
        send_start_time = time.time()
        send_start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        logger.info(f"[ASYNC SERVICE] Отправка PUT запроса на Go бэкенд для LoadSession ID: {session_id}")
        logger.info(f"[ASYNC SERVICE] URL: {GO_SERVICE_URL}")
        logger.info(f"[ASYNC SERVICE] Payload: {json.dumps(result_payload)}")
        sys.stdout.flush()
        
        response = requests.put(GO_SERVICE_URL, json=result_payload, headers=headers, timeout=5)
        
        send_end_time = time.time()
        send_end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        send_duration = send_end_time - send_start_time
        total_duration = send_end_time - start_time
        
        logger.info(f"[ASYNC SERVICE] Получен ответ от Go бэкенда для LoadSession ID: {session_id}")
        logger.info(f"[ASYNC SERVICE] HTTP Status: {response.status_code}")
        logger.info(f"[ASYNC SERVICE] Время отправки запроса: {send_duration:.2f} секунд")
        logger.info(f"[ASYNC SERVICE] Общее время обработки: {total_duration:.2f} секунд")
        sys.stdout.flush()
    except Exception as e:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        logger.error(f"[ASYNC SERVICE] ОШИБКА при отправке на Go бэкенд для LoadSession ID: {session_id}: {e}")
        sys.stdout.flush()


@api_view(['POST'])
def perform_calculation(request):
    """
    Принимает запрос на расчет totalLoad, запускает его в фоне и сразу отвечает 200.
    Ожидает данные в формате:
    {
        "id": <session_id>,
        "loads": [
            {
                "area": <int>,
                "normative": <float>,
                "reliability_coefficient": <float>,
                "load_category": "Постоянная" | "Временная"
            },
            ...
        ]
    }
    """
    receive_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    try:
        data = request.data
        session_id = data.get('id', 'unknown')
        
        logger.info("=" * 60)
        logger.info(f"[ASYNC SERVICE] ПРИЕМ POST запроса на /api/calculate_total_load/")
        logger.info(f"[ASYNC SERVICE] LoadSession ID: {session_id}")
        logger.info(f"[ASYNC SERVICE] Количество нагрузок: {len(data.get('loads', []))}")
        logger.info(f"[ASYNC SERVICE] Запрос принят, запуск асинхронной обработки...")
        logger.info("=" * 60)
        sys.stdout.flush()
        
        if "id" not in data:
            return Response({"error": "No ID provided"}, status=status.HTTP_400_BAD_REQUEST)
        
        if "loads" not in data:
            return Response({"error": "No loads provided"}, status=status.HTTP_400_BAD_REQUEST)
        
        executor.submit(calculate_total_load, data)
        
        logger.info(f"[ASYNC SERVICE] HTTP 200 ответ отправлен клиенту (Go бэкенду)")
        sys.stdout.flush()
        
        return Response({"message": "Calculation started"}, status=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"[ASYNC SERVICE] ОШИБКА при обработке запроса: {e}")
        sys.stdout.flush()
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

