from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import BotCommand, CallbackQuery, FSInputFile, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .charts import render_stability_chart
from .config import Settings, SiteConfig
from .monitor import QAMonitor
from .reporting import short_summary, status_badge
from .storage import Storage, StoredSite


router = Router()
SETTINGS = Settings.from_env()
STORAGE = Storage(SETTINGS.db_path)
STORAGE.bootstrap(SETTINGS)
MONITOR = QAMonitor(SETTINGS, STORAGE)
BOT_HOLDER: dict[str, Bot | None] = {"bot": None}
URL_RE = re.compile(r"^(https?://\S+)$", re.IGNORECASE)


def _is_allowed(user_id: int | None) -> bool:
    if user_id is None:
        return False
    allowed = SETTINGS.telegram_allowed_user_ids
    return True if not allowed else user_id in allowed


async def _ensure_allowed(message: Message | CallbackQuery) -> bool:
    user = message.from_user
    if _is_allowed(user.id if user else None):
        return True
    target = message.message if isinstance(message, CallbackQuery) else message
    await target.answer("Access denied for this bot.")
    return False


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    text = (
        "<b>QA Guard Bot</b>\n\n"
        "Мониторит сайты, сохраняет историю стабильности, умеет запускать ручные проверки и строить графики.\n\n"
        "Команды: /run, /sites, /check, /addsite, /remove, /settings, /chart, /last"
    )
    await message.answer(text, reply_markup=_main_menu_markup())


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    text = (
        "<b>Команды</b>\n"
        "/run — проверить все включённые сайты\n"
        "/sites — список сайтов и действия по ним\n"
        "/check https://site.tld — разовая проверка URL\n"
        "/addsite https://site.tld Имя — добавить сайт в мониторинг\n"
        "/remove 3 — удалить сайт по id\n"
        "/settings — интервал и алерты\n"
        "/chart 3 — график стабильности по id\n"
        "/last — последний отчёт\n\n"
        "Можно просто отправить боту URL — он выполнит разовую проверку."
    )
    await message.answer(text, reply_markup=_main_menu_markup())


@router.message(Command("run"))
async def cmd_run(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    if not [site for site in STORAGE.list_sites() if site.config.enabled]:
        await message.answer("Нет включённых сайтов для мониторинга. Добавь сайт через /addsite или включи существующий.", reply_markup=_main_menu_markup())
        return
    progress = await message.answer("Запускаю проверку всех включённых сайтов.")
    report, json_path, md_path = await MONITOR.run_once(triggered_by=f"telegram:{message.chat.id}")
    await progress.edit_text(short_summary(report), reply_markup=_main_menu_markup())
    await _send_files(message, json_path, md_path, report.run_id)


@router.message(Command("sites"))
async def cmd_sites(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    await _send_sites_list(message)


@router.message(Command("last"))
async def cmd_last(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    report = MONITOR.latest_report
    if report is None:
        await message.answer("Последнего отчёта в памяти ещё нет. Сначала запусти /run.", reply_markup=_main_menu_markup())
        return
    await _send_report_to_message(message, report)


@router.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    await message.answer(_settings_text(), reply_markup=_settings_markup())


@router.message(Command("addsite"))
async def cmd_addsite(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    args = _command_args(message.text)
    if not args:
        await message.answer(
            "Формат: <code>/addsite https://example.com Имя сайта</code>\n"
            "Если имя не указать, возьму hostname. По умолчанию включу crawl на 5 страниц.",
            reply_markup=_main_menu_markup(),
        )
        return
    url, name = _parse_url_and_optional_name(args)
    if not url:
        await message.answer("Не вижу валидный URL. Нужен формат вроде <code>https://example.com</code>.")
        return
    config = _default_site_config(url, name)
    try:
        stored = STORAGE.add_site(config)
    except Exception as exc:
        await message.answer(f"Не удалось добавить сайт: <code>{exc}</code>")
        return
    await message.answer(
        f"Добавлен сайт <b>{stored.config.name}</b> с id <code>{stored.id}</code>.\n"
        f"Crawl: <code>{stored.config.crawl_enabled}</code>, pages: <code>{stored.config.crawl_max_pages}</code>.",
        reply_markup=_site_detail_markup(stored.id),
    )


@router.message(Command("remove"))
async def cmd_remove(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    args = _command_args(message.text)
    if not args or not args.isdigit():
        await message.answer("Формат: <code>/remove 3</code>")
        return
    success = STORAGE.delete_site(int(args))
    await message.answer("Сайт удалён." if success else "Сайт с таким id не найден.", reply_markup=_main_menu_markup())


@router.message(Command("chart"))
async def cmd_chart(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    args = _command_args(message.text)
    if not args or not args.isdigit():
        await message.answer("Формат: <code>/chart 3</code>")
        return
    await _send_chart_for_site(message, int(args))


@router.message(Command("check"))
async def cmd_check(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    args = _command_args(message.text)
    if not args:
        await message.answer("Формат: <code>/check https://example.com</code>")
        return
    url, name = _parse_url_and_optional_name(args)
    if not url:
        await message.answer("Нужен валидный URL для разовой проверки.")
        return
    await _run_adhoc_check(message, _default_site_config(url, name, persist=False))


@router.message(F.text.regexp(URL_RE.pattern))
async def handle_plain_url(message: Message) -> None:
    if not await _ensure_allowed(message):
        return
    text = (message.text or "").strip()
    if not URL_RE.match(text):
        return
    await _run_adhoc_check(message, _default_site_config(text, None, persist=False))


@router.callback_query(F.data == "qa:run")
async def cb_run(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    assert callback.message is not None
    await callback.answer()
    if not [site for site in STORAGE.list_sites() if site.config.enabled]:
        await callback.message.answer("Нет включённых сайтов для мониторинга. Добавь сайт через /addsite или включи существующий.", reply_markup=_main_menu_markup())
        return
    progress = await callback.message.answer("Запускаю проверку всех включённых сайтов.")
    report, json_path, md_path = await MONITOR.run_once(triggered_by=f"telegram:{callback.message.chat.id}")
    await progress.edit_text(short_summary(report), reply_markup=_main_menu_markup())
    await _send_files(callback.message, json_path, md_path, report.run_id)


@router.callback_query(F.data == "qa:sites")
async def cb_sites(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    assert callback.message is not None
    await callback.answer()
    await _send_sites_list(callback.message)


@router.callback_query(F.data == "qa:last")
async def cb_last(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    assert callback.message is not None
    await callback.answer()
    report = MONITOR.latest_report
    if report is None:
        await callback.message.answer("Последнего отчёта в памяти ещё нет. Сначала запусти /run.", reply_markup=_main_menu_markup())
        return
    await _send_report_to_message(callback.message, report)


@router.callback_query(F.data == "qa:settings")
async def cb_settings(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    assert callback.message is not None
    await callback.answer()
    await callback.message.edit_text(_settings_text(), reply_markup=_settings_markup())


@router.callback_query(F.data.startswith("set:int:"))
async def cb_interval(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    delta = int(callback.data.split(":")[-1])
    current = STORAGE.get_check_interval_minutes(SETTINGS.default_check_interval_minutes)
    updated = STORAGE.set_check_interval_minutes(current + delta)
    await callback.answer(f"Интервал: {updated} мин.")
    assert callback.message is not None
    await callback.message.edit_text(_settings_text(), reply_markup=_settings_markup())


@router.callback_query(F.data == "set:notify")
async def cb_notify(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    current = STORAGE.get_notify_only_on_changes(SETTINGS.default_notify_only_on_changes)
    STORAGE.set_notify_only_on_changes(not current)
    await callback.answer("Режим алертов обновлён.")
    assert callback.message is not None
    await callback.message.edit_text(_settings_text(), reply_markup=_settings_markup())


@router.callback_query(F.data == "set:scheduler")
async def cb_scheduler(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    current = STORAGE.get_scheduler_enabled(SETTINGS.default_scheduler_enabled)
    STORAGE.set_scheduler_enabled(not current)
    await callback.answer("Фоновый мониторинг обновлён.")
    assert callback.message is not None
    await callback.message.edit_text(_settings_text(), reply_markup=_settings_markup())


@router.callback_query(F.data.startswith("site:"))
async def cb_site_details(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    site_id = int(callback.data.split(":", 1)[1])
    site = STORAGE.get_site(site_id)
    if site is None:
        await callback.answer("Сайт не найден.", show_alert=True)
        return
    await callback.answer()
    assert callback.message is not None
    await callback.message.edit_text(_site_text(site), reply_markup=_site_detail_markup(site_id))


@router.callback_query(F.data.startswith("sitecheck:"))
async def cb_site_check(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    site_id = int(callback.data.split(":", 1)[1])
    site = STORAGE.get_site(site_id)
    if site is None:
        await callback.answer("Сайт не найден.", show_alert=True)
        return
    assert callback.message is not None
    await callback.answer()
    progress = await callback.message.answer(f"Проверяю <b>{site.config.name}</b>.")
    report, json_path, md_path = await MONITOR.run_once(triggered_by=f"telegram:{callback.message.chat.id}:site:{site_id}", sites=[site])
    await progress.edit_text(short_summary(report), reply_markup=_site_detail_markup(site_id))
    await _send_files(callback.message, json_path, md_path, report.run_id)


@router.callback_query(F.data.startswith("sitechart:"))
async def cb_site_chart(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    site_id = int(callback.data.split(":", 1)[1])
    await callback.answer()
    assert callback.message is not None
    await _send_chart_for_site(callback.message, site_id)


@router.callback_query(F.data.startswith("sitetoggle:"))
async def cb_site_toggle(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    site_id = int(callback.data.split(":", 1)[1])
    site = STORAGE.toggle_site_enabled(site_id)
    if site is None:
        await callback.answer("Сайт не найден.", show_alert=True)
        return
    await callback.answer("Состояние сайта обновлено.")
    assert callback.message is not None
    await callback.message.edit_text(_site_text(site), reply_markup=_site_detail_markup(site_id))


@router.callback_query(F.data.startswith("sitedelete:"))
async def cb_site_delete(callback: CallbackQuery) -> None:
    if not await _ensure_allowed(callback):
        return
    site_id = int(callback.data.split(":", 1)[1])
    success = STORAGE.delete_site(site_id)
    await callback.answer("Сайт удалён." if success else "Сайт не найден.")
    assert callback.message is not None
    await _send_sites_list(callback.message)


def _main_menu_markup():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Run checks", callback_data="qa:run"),
        InlineKeyboardButton(text="Sites", callback_data="qa:sites"),
    )
    builder.row(
        InlineKeyboardButton(text="Settings", callback_data="qa:settings"),
        InlineKeyboardButton(text="Last report", callback_data="qa:last"),
    )
    return builder.as_markup()



def _settings_markup():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="-5 min", callback_data="set:int:-5"),
        InlineKeyboardButton(text="+5 min", callback_data="set:int:5"),
        InlineKeyboardButton(text="+15 min", callback_data="set:int:15"),
    )
    builder.row(
        InlineKeyboardButton(text="Toggle alerts mode", callback_data="set:notify"),
        InlineKeyboardButton(text="Toggle scheduler", callback_data="set:scheduler"),
    )
    builder.row(InlineKeyboardButton(text="Back", callback_data="qa:sites"))
    return builder.as_markup()



def _site_detail_markup(site_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Check", callback_data=f"sitecheck:{site_id}"),
        InlineKeyboardButton(text="Chart", callback_data=f"sitechart:{site_id}"),
    )
    builder.row(
        InlineKeyboardButton(text="Enable/Disable", callback_data=f"sitetoggle:{site_id}"),
        InlineKeyboardButton(text="Delete", callback_data=f"sitedelete:{site_id}"),
    )
    builder.row(InlineKeyboardButton(text="Back to sites", callback_data="qa:sites"))
    return builder.as_markup()


async def _send_sites_list(message: Message) -> None:
    sites = STORAGE.list_sites()
    if not sites:
        await message.answer("Сайтов в мониторинге пока нет. Добавь через /addsite https://example.com Имя.", reply_markup=_main_menu_markup())
        return
    lines = ["<b>Configured sites</b>", ""]
    builder = InlineKeyboardBuilder()
    for site in sites:
        history = STORAGE.get_site_history(site.id, limit=1)
        last_status = history[-1]["status"].upper() if history else "NO DATA"
        lines.append(
            f"<b>{site.id}. {site.config.name}</b> — <code>{'enabled' if site.config.enabled else 'disabled'}</code> — {last_status}"
        )
        lines.append(f"<code>{site.config.url}</code>")
        lines.append(
            f"crawl=<code>{site.config.crawl_enabled}</code>, pages=<code>{site.config.crawl_max_pages}</code>, subdomains=<code>{site.config.include_subdomains}</code>"
        )
        lines.append("")
        builder.row(InlineKeyboardButton(text=f"{site.id}. {site.config.name}", callback_data=f"site:{site.id}"))
    builder.row(InlineKeyboardButton(text="Back to menu", callback_data="qa:settings"))
    await message.answer("\n".join(lines).strip(), reply_markup=builder.as_markup())


async def _run_adhoc_check(message: Message, site: SiteConfig) -> None:
    progress = await message.answer(f"Разовая проверка <code>{site.url}</code>.")
    report, json_path, md_path = await MONITOR.run_once(triggered_by=f"adhoc:{message.chat.id}", ad_hoc_sites=[site])
    await progress.edit_text(short_summary(report), reply_markup=_main_menu_markup())
    await _send_files(message, json_path, md_path, report.run_id)


async def _send_report_to_message(message: Message, report) -> None:
    await message.answer(short_summary(report), reply_markup=_main_menu_markup())
    json_path = SETTINGS.reports_dir / "latest_report.json"
    md_path = SETTINGS.reports_dir / "latest_report.md"
    if json_path.exists() and md_path.exists():
        await _send_files(message, json_path, md_path, report.run_id)


async def _send_chart_for_site(message: Message, site_id: int) -> None:
    site = STORAGE.get_site(site_id)
    if site is None:
        await message.answer("Сайт не найден.")
        return
    history = STORAGE.get_site_history(site_id, limit=40)
    if len(history) < 2:
        await message.answer("Для графика пока мало истории. Нужны хотя бы два прогона.")
        return
    chart_path = SETTINGS.reports_dir / "charts" / f"site-{site_id}.png"
    render_stability_chart(site.config.name, history, chart_path)
    await message.answer_photo(
        FSInputFile(str(chart_path), filename=f"stability-site-{site_id}.png"),
        caption=f"График стабильности для <b>{site.config.name}</b>",
    )


async def _send_files(message: Message, json_path: Path, md_path: Path, run_id: str) -> None:
    await message.answer_document(
        FSInputFile(path=str(md_path), filename=f"qa-report-{run_id}.md"),
        caption="Полный человекочитаемый отчёт",
    )
    await message.answer_document(
        FSInputFile(path=str(json_path), filename=f"qa-report-{run_id}.json"),
        caption="Машиночитаемый отчёт",
    )


async def _send_background_report(report, json_path, md_path, error_text: str | None = None) -> None:
    bot = BOT_HOLDER["bot"]
    if bot is None:
        return
    for chat_id in SETTINGS.notify_chat_ids:
        try:
            if error_text:
                await bot.send_message(chat_id=chat_id, text=error_text)
                continue
            assert report is not None and json_path is not None and md_path is not None
            await bot.send_message(chat_id=chat_id, text=short_summary(report))
            await bot.send_document(
                chat_id=chat_id,
                document=FSInputFile(path=str(md_path), filename=f"qa-report-{report.run_id}.md"),
                caption="Полный человекочитаемый отчёт",
            )
        except Exception as exc:
            logging.exception("Failed to notify chat %s: %s", chat_id, exc)



def _command_args(text: str | None) -> str:
    if not text:
        return ""
    parts = text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""



def _parse_url_and_optional_name(value: str) -> tuple[str | None, str | None]:
    value = value.strip()
    if not value:
        return None, None
    parts = value.split(maxsplit=1)
    url = parts[0].strip()
    if not URL_RE.match(url):
        return None, None
    name = parts[1].strip() if len(parts) > 1 else None
    return url, name or None



def _default_site_config(url: str, name: str | None, persist: bool = True) -> SiteConfig:
    hostname = urlparse(url).hostname or url
    return SiteConfig(
        name=(name or hostname).strip(),
        url=url.strip(),
        soft_expectations="The page should look like a legitimate public product page with readable content and no obvious layout breakage.",
        max_console_errors=0,
        max_page_errors=0,
        max_request_failures=0,
        min_visible_text_chars=80,
        llm_enabled=True,
        screenshot=True,
        crawl_enabled=True if persist else False,
        crawl_max_pages=5 if persist else 1,
        include_subdomains=False,
    )



def _settings_text() -> str:
    interval = STORAGE.get_check_interval_minutes(SETTINGS.default_check_interval_minutes)
    notify_mode = STORAGE.get_notify_only_on_changes(SETTINGS.default_notify_only_on_changes)
    scheduler_enabled = STORAGE.get_scheduler_enabled(SETTINGS.default_scheduler_enabled)
    return (
        "<b>Monitoring settings</b>\n\n"
        f"Scheduler: <code>{'enabled' if scheduler_enabled else 'disabled'}</code>\n"
        f"Interval: <code>{interval} min</code>\n"
        f"Alerts only on changes: <code>{notify_mode}</code>\n"
        f"Notify chats: <code>{', '.join(map(str, SETTINGS.notify_chat_ids)) or 'not set'}</code>"
    )



def _site_text(site: StoredSite) -> str:
    history = STORAGE.get_site_history(site.id, limit=3)
    last_lines = []
    for row in history:
        last_lines.append(f"- {row['created_at']} — <b>{str(row['status']).upper()}</b> — {row['duration_ms']} ms")
    history_block = "\n".join(last_lines) if last_lines else "- no history yet"
    return (
        f"<b>{site.config.name}</b>\n"
        f"id: <code>{site.id}</code>\n"
        f"url: <code>{site.config.url}</code>\n"
        f"enabled: <code>{site.config.enabled}</code>\n"
        f"crawl: <code>{site.config.crawl_enabled}</code> / pages <code>{site.config.crawl_max_pages}</code>\n"
        f"include subdomains: <code>{site.config.include_subdomains}</code>\n"
        f"llm: <code>{site.config.llm_enabled}</code>\n\n"
        f"<b>Recent history</b>\n{history_block}"
    )



def _main_menu_markup():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="Run checks", callback_data="qa:run"),
        InlineKeyboardButton(text="Sites", callback_data="qa:sites"),
    )
    builder.row(
        InlineKeyboardButton(text="Settings", callback_data="qa:settings"),
        InlineKeyboardButton(text="Last report", callback_data="qa:last"),
    )
    return builder.as_markup()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    bot = Bot(token=SETTINGS.telegram_bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    BOT_HOLDER["bot"] = bot
    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Open the bot menu"),
            BotCommand(command="run", description="Run checks for all enabled sites"),
            BotCommand(command="sites", description="Show configured sites"),
            BotCommand(command="check", description="Run one-off check for a URL"),
            BotCommand(command="addsite", description="Add a site to monitoring"),
            BotCommand(command="remove", description="Delete a monitored site by id"),
            BotCommand(command="settings", description="Monitoring interval and alerts"),
            BotCommand(command="chart", description="Show stability chart for a site"),
            BotCommand(command="last", description="Send the last report"),
            BotCommand(command="help", description="Show help"),
        ]
    )

    background_task = asyncio.create_task(MONITOR.periodic_loop(_send_background_report))
    try:
        await dispatcher.start_polling(bot)
    finally:
        background_task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
