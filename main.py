import asyncio
import json
import math
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
from astrbot.api.star import Context, Star, register
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api import logger
import astrbot.api.message_components as Comp


@register("earthquake_weather_alert", "user", "地震+气象预警推送", "1.0.0")
class EarthquakeWeatherAlert(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.plugin_dir = os.path.dirname(os.path.abspath(__file__))
        self.state_path = os.path.join(self.plugin_dir, "state.json")
        self.state = self._load_state()
        
        self.location_id: Optional[str] = None
        self.user_lat: Optional[float] = None
        self.user_lon: Optional[float] = None
        
        self._eq_task: Optional[asyncio.Task] = None
        self._warn_task: Optional[asyncio.Task] = None
        self._daily_task: Optional[asyncio.Task] = None
        
        self._stats = {
            "eq_pushed_total": len(self.state.get("pushed_earthquakes", {})),
            "warn_pushed_total": len(self.state.get("pushed_warnings", [])),
            "last_eq_check": "从未",
            "last_warn_check": "从未",
            "last_weather_push": "从未",
            "initialized": False,
            "location_resolved": False
        }
        
    def _load_state(self):
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "pushed_earthquakes": {},
            "pushed_warnings": []
        }
    
    def _save_state(self):
        try:
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[EWA] 状态保存失败: {e}")
    
    def _clean_state(self):
        eq = self.state.get("pushed_earthquakes", {})
        if len(eq) > 200:
            items = sorted(eq.items(), key=lambda x: x[0], reverse=True)
            self.state["pushed_earthquakes"] = dict(items[:150])
        warns = self.state.get("pushed_warnings", [])
        if len(warns) > 500:
            self.state["pushed_warnings"] = warns[-400:]
        self._save_state()
    
    def _get_qweather_host(self):
        host = self.config.get("qweather_host", "").strip()
        if not host:
            logger.warning("[EWA] qweather_host 未配置，尝试使用公共域名（可能已失效）")
            return "geoapi.qweather.com"
        return host.rstrip("/")
    
    # ========== 和风初始化 ==========
    
    async def _init_qweather(self):
        key = self.config.get("qweather_key", "")
        if not key:
            logger.error("[EWA] 和风 Key 未配置，请在 WebUI 插件配置中填写")
            return False
        
        addr = self.config.get("address", "")
        if not addr:
            logger.error("[EWA] 监测地址未配置，请在 WebUI 插件配置中填写")
            return False
        
        host = self._get_qweather_host()
        url = f"https://{host}/geo/v2/city/lookup?location={addr}&key={key}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    data = await resp.json()
                    if data.get("code") == "200" and data.get("location"):
                        loc = data["location"][0]
                        self.location_id = loc["id"]
                        self.user_lat = float(loc["lat"])
                        self.user_lon = float(loc["lon"])
                        self._stats["location_resolved"] = True
                        logger.info(f"[EWA] 位置解析: {loc['name']} ({self.location_id}), "
                                    f"坐标: {self.user_lat}, {self.user_lon}")
                        return True
                    else:
                        logger.error(f"[EWA] GeoAPI 失败: {data}")
                        return False
        except Exception as e:
            logger.error(f"[EWA] GeoAPI 异常: {e}")
            return False
    
    # ========== 发送消息 ==========
    
    async def _send(self, text: str):
        qq = self.config.get("target_qq", "")
        if not qq:
            logger.error("[EWA] 目标 QQ 未配置，无法推送")
            return
        
        chain = MessageChain([Comp.Plain(text)])
        
        # 收集所有可能的平台 ID
        platform_ids = []
        
        # 1. 用户配置的平台 ID
        cfg_pid = self.config.get("platform_id", "").strip()
        if cfg_pid:
            platform_ids.append(cfg_pid)
        
        # 2. 尝试从 context 探测
        try:
            pm = getattr(self.context, 'platform_manager', None)
            if pm:
                for inst in getattr(pm, 'platform_insts', []):
                    pid = getattr(getattr(inst, 'meta', lambda: None)(), 'id', None)
                    if pid and pid not in platform_ids:
                        platform_ids.append(pid)
                        logger.info(f"[EWA] 探测到平台 ID: {pid}")
        except Exception as e:
            logger.debug(f"[EWA] 探测平台失败: {e}")
        
        # 3. 常见默认值
        for pid in ['aiocqhttp', 'default', 'qq', 'napcat']:
            if pid not in platform_ids:
                platform_ids.append(pid)
        
        # 去重
        seen = set()
        unique_ids = []
        for pid in platform_ids:
            if pid and pid not in seen:
                seen.add(pid)
                unique_ids.append(pid)
        
        logger.info(f"[EWA] 尝试平台 ID 列表: {unique_ids}")
        
        # 尝试所有组合
        for pid in unique_ids:
            for msg_type in ['PrivateMessage', 'FriendMessage']:
                umo = f"{pid}:{msg_type}:{qq}"
                try:
                    result = await self.context.send_message(umo, chain)
                    if result:
                        logger.info(f"[EWA] 已推送到 {umo}")
                        return
                except Exception as e:
                    logger.debug(f"[EWA] {umo} 失败: {e}")
        
        logger.error("[EWA] 所有平台发送方式均失败，请检查：1.机器人QQ是否加目标QQ为好友 2.在群聊发送/sid查看正确会话格式 3.在配置中填写platform_id")
    
    # ========== 地震模块 ==========
    
    async def _poll_earthquake(self):
        while True:
            try:
                await self._check_earthquake()
                self._stats["last_eq_check"] = datetime.now(
                    timezone(timedelta(hours=8))
                ).strftime("%m-%d %H:%M:%S")
            except Exception as e:
                logger.error(f"[EWA] 地震轮询异常: {e}")
            await asyncio.sleep(self.config.get("earthquake_poll_interval", 30))
    
    async def _check_earthquake(self):
        url = "https://mobile-new.chinaeew.cn/v1/earlywarnings?updates=20&start_at=0"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        
        if data.get("code") != 0:
            return
        
        events = data.get("data", [])
        if not events:
            return
        
        events.sort(key=lambda x: x.get("startAt", 0), reverse=True)
        
        for event in events:
            eid = event.get("eventId")
            updates = event.get("updates", 1)
            mag = event.get("magnitude", 0)
            
            if mag < self.config.get("min_magnitude", 3.0):
                continue
            
            last = self.state["pushed_earthquakes"].get(str(eid), 0)
            if updates <= last:
                continue
            
            self.state["pushed_earthquakes"][str(eid)] = updates
            self._save_state()
            self._clean_state()
            self._stats["eq_pushed_total"] = len(self.state["pushed_earthquakes"])
            await self._dispatch_earthquake(event)
    
    def _calc_distance(self, lat1, lon1, lat2, lon2):
        R = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    
    def _calc_intensity(self, mag, dist):
        D = math.sqrt(dist ** 2 + 25)
        intensity = 1.5 * mag - 3.5 * math.log10(D) + 3.0
        return max(1.0, min(12.0, intensity))
    
    def _format_earthquake(self, event: dict) -> str:
        eid = event.get("eventId", "N/A")
        mag = event.get("magnitude", 0)
        loc = event.get("epicenter", "未知位置")
        depth = event.get("depth", "未知")
        lat = event.get("latitude", 0)
        lon = event.get("longitude", 0)
        updates = event.get("updates", 1)
        
        source_type = event.get("sourceType")
        if source_type == 1:
            source = "大陆地震预警网"
        elif source_type == 2:
            source = "中国地震台网"
        else:
            source = "地震预警系统"
        
        ts = event.get("startAt", 0)
        if ts > 1e11:
            ts = ts / 1000
        dt = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8))).strftime("%m-%d %H:%M:%S")
        
        dist_str = ""
        intensity_str = ""
        arrival_str = ""
        if self.user_lat is not None and self.user_lon is not None:
            dist = self._calc_distance(self.user_lat, self.user_lon, lat, lon)
            intensity = self._calc_intensity(mag, dist)
            
            dist_str = f"\n📐 距你约：{dist:.1f} km"
            intensity_str = f"\n💥 预估烈度：{intensity:.1f} 度"
            
            p_arrival = dist / 6.0
            s_arrival = dist / 3.5
            arrival_str = f"\n⏱ P波约 {p_arrival:.0f}s 后到达，S波约 {s_arrival:.0f}s 后到达"
        
        update_tag = f" [第{updates}次更新]" if updates > 1 else ""
        
        mag_emoji = "🟢"
        if mag >= 4.5: mag_emoji = "🟠"
        if mag >= 5.0: mag_emoji = "🔴"
        if mag >= 6.0: mag_emoji = "⚫"
        
        msg = (
            f"{mag_emoji} 地震预警{update_tag}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📍 震中：{loc}\n"
            f"🌡 震级：{mag} 级\n"
            f"📏 深度：{depth} km"
            f"{dist_str}"
            f"{intensity_str}"
            f"{arrival_str}\n"
            f"🕐 时间：{dt}\n"
            f"📡 来源：{source}\n"
            f"🗺 坐标：{lat:.3f}, {lon:.3f}\n"
            f"🆔 EventID: {eid}"
        )
        return msg
    
    async def _dispatch_earthquake(self, event: dict):
        msg = self._format_earthquake(event)
        await self._send(msg)
    
    # ========== 气象预警模块 ==========
    
    async def _poll_warning(self):
        while True:
            try:
                await self._check_warning()
                self._stats["last_warn_check"] = datetime.now(
                    timezone(timedelta(hours=8))
                ).strftime("%m-%d %H:%M:%S")
            except Exception as e:
                logger.error(f"[EWA] 预警轮询异常: {e}")
            await asyncio.sleep(self.config.get("warning_poll_interval", 300))
    
    async def _check_warning(self):
        if not self.location_id:
            logger.warning("[EWA] 位置未初始化，跳过预警检查")
            return
        
        key = self.config.get("qweather_key", "")
        host = self._get_qweather_host()
        url = f"https://{host}/v7/warning/now?location={self.location_id}&key={key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
        
        if data.get("code") != "200":
            return
        
        warnings = data.get("warning", [])
        if not warnings:
            return
        
        min_level = self.config.get("min_warning_level", "蓝色")
        level_order = {"蓝色": 1, "黄色": 2, "橙色": 3, "红色": 4}
        min_val = level_order.get(min_level, 1)
        
        pushed = set(self.state.get("pushed_warnings", []))
        new_pushed = []
        
        for w in warnings:
            wid = w.get("id", "")
            level = w.get("level", "蓝色")
            if level_order.get(level, 0) < min_val:
                continue
            
            if wid not in pushed:
                self.state["pushed_warnings"].append(wid)
                new_pushed.append(w)
        
        if new_pushed:
            self._save_state()
            self._clean_state()
            self._stats["warn_pushed_total"] = len(self.state["pushed_warnings"])
            for w in new_pushed:
                await self._dispatch_warning(w)
    
    def _format_warning(self, w: dict) -> str:
        level = w.get("level", "未知")
        title = w.get("title", "")
        text = w.get("text", "")
        pub_time = w.get("pubTime", "")
        
        if pub_time:
            try:
                dt = datetime.fromisoformat(pub_time.replace("Z", "+00:00"))
                dt = dt.astimezone(timezone(timedelta(hours=8)))
                pub_time = dt.strftime("%m-%d %H:%M")
            except Exception:
                pass
        
        emoji = {"蓝色": "🔵", "黄色": "🟡", "橙色": "🟠", "红色": "🔴"}.get(level, "⚪")
        
        msg = (
            f"{emoji} {title}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🕐 发布时间：{pub_time}\n"
            f"📋 详情：{text}"
        )
        return msg
    
    async def _dispatch_warning(self, w: dict):
        msg = self._format_warning(w)
        await self._send(msg)
    
    # ========== 天气查询 ==========
    
    async def _query_weather(self, location_id: str) -> str:
        key = self.config.get("qweather_key", "")
        host = self._get_qweather_host()
        url = f"https://{host}/v7/weather/now?location={location_id}&key={key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
        
        if data.get("code") != "200":
            return f"天气查询失败: {data.get('code')}"
        
        now = data.get("now", {})
        obs_time = now.get("obsTime", "")
        temp = now.get("temp", "N/A")
        feels = now.get("feelsLike", "N/A")
        text = now.get("text", "N/A")
        wind_dir = now.get("windDir", "N/A")
        wind_scale = now.get("windScale", "N/A")
        humidity = now.get("humidity", "N/A")
        vis = now.get("vis", "N/A")
        
        addr = self.config.get("address", "当前位置")
        return (
            f"🌤 {addr} 实时天气\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🌡 温度：{temp}°C（体感 {feels}°C）\n"
            f"☁ 天气：{text}\n"
            f"💨 风向：{wind_dir} {wind_scale}级\n"
            f"💧 湿度：{humidity}%\n"
            f"👁 能见度：{vis} km\n"
            f"🕐 观测：{obs_time}"
        )
    
    async def _query_daily_weather(self, location_id: str) -> str:
        key = self.config.get("qweather_key", "")
        host = self._get_qweather_host()
        addr = self.config.get("address", "当前位置")
        
        now_url = f"https://{host}/v7/weather/now?location={location_id}&key={key}"
        day_url = f"https://{host}/v7/weather/3d?location={location_id}&key={key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(now_url, timeout=10) as resp:
                now_data = await resp.json()
            async with session.get(day_url, timeout=10) as resp:
                day_data = await resp.json()
        
        if now_data.get("code") != "200" or day_data.get("code") != "200":
            return f"每日天气查询失败: now={now_data.get('code')} day={day_data.get('code')}"
        
        now = now_data.get("now", {})
        daily = day_data.get("daily", [{}])[0]
        
        temp = now.get("temp", "N/A")
        feels = now.get("feelsLike", "N/A")
        text = now.get("text", "N/A")
        wind_dir = now.get("windDir", "N/A")
        wind_scale = now.get("windScale", "N/A")
        humidity = now.get("humidity", "N/A")
        
        max_temp = daily.get("tempMax", "N/A")
        min_temp = daily.get("tempMin", "N/A")
        day_text = daily.get("textDay", "N/A")
        night_text = daily.get("textNight", "N/A")
        fx_date = daily.get("fxDate", "")
        
        weekday_map = ["一", "二", "三", "四", "五", "六", "日"]
        try:
            dt = datetime.strptime(fx_date, "%Y-%m-%d")
            weekday = f"星期{weekday_map[dt.weekday()]}"
            date_str = dt.strftime("%m月%d日")
        except Exception:
            weekday = ""
            date_str = fx_date
        
        return (
            f"🌤 早安，{addr}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📅 {date_str} {weekday}\n"
            f"🌡 实时：{temp}°C（体感 {feels}°C）{text}\n"
            f"📈 预报：{min_temp}°C ~ {max_temp}°C，白天{day_text}夜间{night_text}\n"
            f"💨 风向：{wind_dir} {wind_scale}级\n"
            f"💧 湿度：{humidity}%"
        )
    
    # ========== 每日天气推送 ==========
    
    async def _poll_daily_weather(self):
        push_time = self.config.get("daily_weather_push_time", "").strip()
        if not push_time:
            logger.info("[EWA] 每日天气推送已关闭（未设置时间）")
            return
        
        try:
            hour, minute = map(int, push_time.split(":"))
        except Exception:
            logger.error(f"[EWA] 每日推送时间格式错误: {push_time}，应为 HH:MM")
            return
        
        while True:
            now = datetime.now(timezone(timedelta(hours=8)))
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            
            sleep_seconds = (target - now).total_seconds()
            logger.info(f"[EWA] 每日天气推送将在 {target.strftime('%m-%d %H:%M')} 执行，"
                        f"距离 {sleep_seconds/3600:.1f} 小时")
            await asyncio.sleep(sleep_seconds)
            
            if self.location_id:
                try:
                    msg = await self._query_daily_weather(self.location_id)
                    await self._send(msg)
                    self._stats["last_weather_push"] = datetime.now(
                        timezone(timedelta(hours=8))
                    ).strftime("%m-%d %H:%M:%S")
                    logger.info("[EWA] 每日天气推送完成")
                except Exception as e:
                    logger.error(f"[EWA] 每日天气推送异常: {e}")
            
            await asyncio.sleep(61)
    
    # ========== 生命周期 ==========
    
    async def initialize(self):
        await asyncio.sleep(3)
        
        key = self.config.get("qweather_key", "")
        qq = self.config.get("target_qq", "")
        
        if not key:
            logger.error("[EWA] 和风 Key 未配置，请在 WebUI 配置后重载插件")
            return
        if not qq:
            logger.error("[EWA] 目标 QQ 未配置，请在 WebUI 配置后重载插件")
            return
        
        ok = await self._init_qweather()
        if ok:
            self._stats["initialized"] = True
            await self._init_mark_history()
            self._eq_task = asyncio.create_task(self._poll_earthquake())
            self._warn_task = asyncio.create_task(self._poll_warning())
            self._daily_task = asyncio.create_task(self._poll_daily_weather())
            logger.info("[EWA] 地震+气象预警+每日天气推送已启动")
        else:
            logger.error("[EWA] 初始化失败，插件未启动轮询")
    
    async def _init_mark_history(self):
        """初始化时把当前接口里的历史地震全部标记为已推送，避免重启刷屏"""
        try:
            url = "https://mobile-new.chinaeew.cn/v1/earlywarnings?updates=20&start_at=0"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("code") == 0:
                            for event in data.get("data", []):
                                eid = event.get("eventId")
                                updates = event.get("updates", 1)
                                if eid:
                                    self.state["pushed_earthquakes"][str(eid)] = updates
                            self._save_state()
                            logger.info(f"[EWA] 已标记 {len(self.state['pushed_earthquakes'])} 条历史地震为已读")
        except Exception as e:
            logger.warning(f"[EWA] 标记历史地震失败: {e}")
    
    async def terminate(self):
        for t in [self._eq_task, self._warn_task, self._daily_task]:
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
    
    # ========== 指令 ==========
    
    @filter.command("天气")
    async def cmd_weather(self, event: AstrMessageEvent):
        if not self.location_id:
            yield event.plain_result("位置未初始化，请检查 WebUI 配置")
            return
        result = await self._query_weather(self.location_id)
        yield event.plain_result(result)
    
    @filter.command("地震测试")
    async def cmd_eq_test(self, event: AstrMessageEvent):
        addr = self.config.get("address", "测试地点")
        test = {
            "eventId": 999999,
            "updates": 1,
            "magnitude": 5.2,
            "epicenter": f"{addr}（测试）",
            "depth": 10,
            "latitude": self.user_lat or 30.67,
            "longitude": self.user_lon or 104.07,
            "epiIntensity": 7.0,
            "startAt": int(time.time() * 1000),
            "sourceType": 1
        }
        msg = self._format_earthquake(test)
        await self._send(msg)
        yield event.plain_result("地震测试消息已推送，请检查私聊")
    
    @filter.command("预警测试")
    async def cmd_warn_test(self, event: AstrMessageEvent):
        addr = self.config.get("address", "测试地点")
        test = {
            "id": "TEST001",
            "level": "橙色",
            "typeName": "暴雨",
            "title": f"{addr}暴雨橙色预警（测试）",
            "text": "预计未来3小时降雨量将达50毫米以上，请注意防范。",
            "pubTime": datetime.now(timezone(timedelta(hours=8))).isoformat()
        }
        msg = self._format_warning(test)
        await self._send(msg)
        yield event.plain_result("预警测试消息已推送，请检查私聊")
    
    @filter.command("预警状态")
    async def cmd_status(self, event: AstrMessageEvent):
        loc = self.location_id or "未初始化"
        lat = self.user_lat or "N/A"
        lon = self.user_lon or "N/A"
        qq = self.config.get("target_qq", "未设置")
        addr = self.config.get("address", "未设置")
        push_time = self.config.get("daily_weather_push_time", "未设置")
        yield event.plain_result(
            f"📊 插件状态\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📍 地址：{addr}\n"
            f"🌐 位置ID：{loc}\n"
            f"🗺 坐标：{lat}, {lon}\n"
            f"🌋 已推送地震：{self._stats['eq_pushed_total']} 条\n"
            f"⚠ 已推送预警：{self._stats['warn_pushed_total']} 条\n"
            f"🕐 最后地震检查：{self._stats['last_eq_check']}\n"
            f"🕐 最后预警检查：{self._stats['last_warn_check']}\n"
            f"🌤 最后天气推送：{self._stats['last_weather_push']}\n"
            f"⏰ 每日推送时间：{push_time}\n"
            f"🎯 目标：私聊 {qq}"
        )