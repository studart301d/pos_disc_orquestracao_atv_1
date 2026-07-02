"""Callbacks de ciclo de vida das tasks, simulando envio de alertas."""

import logging

log = logging.getLogger("shopbrasil.alerts")


def _descrever(context) -> str:
    instancia = context["task_instance"]
    return f"dag={instancia.dag_id} task={instancia.task_id} run={context['run_id']}"


def notificar_falha(context):
    # Em produção este callback enviaria mensagem para Slack/e-mail do time.
    log.error("[ALERTA][FALHA] Pipeline falhou: %s", _descrever(context))


def notificar_retry(context):
    tentativa = context["task_instance"].try_number
    log.warning("[ALERTA][RETRY] Nova tentativa (try %s): %s", tentativa, _descrever(context))


def notificar_sucesso(context):
    log.info("[ALERTA][OK] Task concluída com sucesso: %s", _descrever(context))
