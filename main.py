import os
import db
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

botToken = os.getenv('BOT_TOKEN')

async def welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    userCode = update.message.from_user.id
    firstName = update.message.from_user.first_name
    lastName = update.message.from_user.last_name
    username = update.message.from_user.username

    with db.SessionLocal() as dbSession:
        try:
            if db.checkUserInBlocked(userCode, dbSession):
                await update.message.reply_text("Доступ запрещен. Вы заблокированы.")
            elif db.checkUserInDb(userCode, dbSession):
                await update.message.reply_text(f"Привет, {firstName} {lastName or ''}! Вы уже зарегистрированы.")
            else:
                db.addUserToDb(userCode, username, firstName, lastName, dbSession)
                await update.message.reply_text(f"Вы успешно зарегистрированы, {firstName} {lastName or ''}!")
        except Exception as e:
            await update.message.reply_text(f"Ошибка при регистрации. Пожалуйста, попробуйте позже. {str(e)}")


def main():
    if not botToken:
        print("Ошибка: токен бота не найден в переменных окружения!")
        return

    db.createTables()

    application = Application.builder().token(botToken).build()

    application.add_handler(CommandHandler("start", welcome))

    application.run_polling()

if __name__ == '__main__':
    main()











