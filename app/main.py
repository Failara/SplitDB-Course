import time
import random
from prometheus_client import start_http_server, Gauge, Counter

# === КОНФІГУРАЦІЯ ===
UPDATE_PERIOD = 5  # секунд
# Симуляція: 1 "день" проходить за кілька хвилин для наочності

# === МЕТРИКИ PROMETHEUS (Варіант 10, Підваріант В) === [cite: 1370, 1431-1435]

# Ринкова ціна (Day-ahead market price)
market_price = Gauge('bess_market_price_eur_mwh', 'Поточна ринкова ціна на електроенергію (€/МВт-год)')

# Потужність (+ зарядка, - розрядка для продажу)
bess_power = Gauge('bess_power_mw', 'Потужність BESS (МВт). Позитивна - розрядка в мережу, негативна - зарядка.')

# SOC (Рівень заряду)
bess_soc = Gauge('bess_soc_percent', 'Рівень заряду батареї (%)')

# Економічні показники
daily_revenue = Gauge('bess_daily_revenue_eur', 'Накопичений прибуток за добу (€)')
degradation_cost = Gauge('bess_daily_degradation_cost_eur', 'Накопичена вартість деградації за добу (€)')
arbitrage_opportunity = Gauge('bess_arbitrage_spread', 'Різниця цін (спред) для арбітражу (€)')

# Внутрішні змінні стану
current_soc = 50.0  # %
revenue_accumulated = 0.0
degradation_accumulated = 0.0
battery_capacity_mwh = 20.0  # Ємність батареї
max_power_mw = 10.0          # Макс потужність

def simulate_market_logic():
    global current_soc, revenue_accumulated, degradation_accumulated

    # 1. Симуляція ринкової ціни (коливається від 10 до 200 євро)
    # Вранці та ввечері ціна вища, вночі нижча
    hour_simulated = (time.time() / 5) % 24  # Прискорений час
    base_price = 40
    if 8 < hour_simulated < 22:
        base_price = 140 # Пік
    
    current_price = random.gauss(base_price, 20)
    current_price = max(10, current_price) # Ціна не може бути < 10
    
    market_price.set(current_price)

    # 2. Логіка "Розумного трейдингу" (Arbitrage Logic)
    # Якщо ціна низька -> Заряджаємо (купуємо)
    # Якщо ціна висока -> Розряджаємо (продаємо)
    
    power_setpoint = 0.0
    
    if current_price < 50 and current_soc < 90:
        # Купуємо енергію
        power_setpoint = -random.uniform(5, max_power_mw) 
    elif current_price > 120 and current_soc > 10:
        # Продаємо енергію
        power_setpoint = random.uniform(5, max_power_mw)
    else:
        # Standby (ціна середня)
        power_setpoint = 0.0

    bess_power.set(power_setpoint)

    # 3. Оновлення SOC
    # Power (MW) * time (h) = MWh. Тут time - це крок симуляції
    energy_change_mwh = (power_setpoint * (UPDATE_PERIOD / 3600)) 
    
    # Якщо заряджаємо (power < 0), SOC росте. Якщо розряджаємо, падає.
    # Але power_setpoint < 0 означає споживання з мережі.
    soc_change = -(energy_change_mwh / battery_capacity_mwh) * 100
    current_soc += soc_change
    current_soc = max(0, min(100, current_soc))
    bess_soc.set(current_soc)

    # 4. Розрахунок прибутку (Revenue Tracking)
    # Прибуток = Продана енергія * Ціна - Куплена енергія * Ціна
    # Якщо power > 0 (продаж), flow_money (+). Якщо power < 0 (купівля), flow_money (-).
    money_flow = (power_setpoint * (UPDATE_PERIOD / 3600)) * current_price
    revenue_accumulated += money_flow
    daily_revenue.set(revenue_accumulated)

    # 5. Вартість деградації (Degradation Cost)
    # Кожен МВт роботи зношує батарею. Припустимо 10 євро за МВт-год пропущеної енергії
    deg_step = abs(energy_change_mwh) * 10.0 
    degradation_accumulated += deg_step
    degradation_cost.set(degradation_accumulated)

    # 6. Arbitrage Spread (для візуалізації можливостей)
    # Різниця між поточною ціною та "ідеальною" ціною покупки (наприклад, 30 євро)
    spread = current_price - 30
    arbitrage_opportunity.set(spread)

    print(f"Price: {current_price:.1f}€ | Power: {power_setpoint:.1f}MW | SOC: {current_soc:.1f}% | Rev: {revenue_accumulated:.1f}€")

if __name__ == '__main__':
    # Запуск HTTP сервера для Prometheus
    start_http_server(8000)
    print("Prometheus metrics server running on port 8000")
    
    while True:
        simulate_market_logic()
        time.sleep(UPDATE_PERIOD)