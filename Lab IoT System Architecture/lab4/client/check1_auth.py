"""Проверка 1. Аутентификация: password_file, анонимы запрещены, пользователи device01/gateway01/svc-ingest/dashboard."""
from common import A, V3, V5, Check, Client, pw


def attempt(user, password, version):
    c = Client(A, user, password, version=version).connect()
    res = (c.ok, c.connack_str())
    c.close()
    return res


def main():
    ch = Check(1, "Аутентификация (password_file)")
    ok, rc = attempt(None, None, V3)
    ch.row("анонимное подключение (MQTT 3.1.1)", "отказ", rc, not ok)
    ok, rc = attempt(None, None, V5)
    ch.row("анонимное подключение (MQTT 5)", "отказ: Not authorized / Bad user name or password", rc, not ok)
    ok, rc = attempt("device01", "wrong-password", V5)
    ch.row("device01 с неверным паролем", "отказ", rc, not ok)
    ok, rc = attempt("hacker", "123", V5)
    ch.row("несуществующий пользователь hacker", "отказ", rc, not ok)
    for user in ("device01", "gateway01", "svc-ingest", "dashboard"):
        ok, rc = attempt(user, pw(user), V5)
        ch.row(f"{user} с верным паролем", "Success", rc, ok)
    return ch.save()


if __name__ == "__main__":
    main()
