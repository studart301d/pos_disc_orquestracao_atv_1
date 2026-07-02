from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):

    template_fields = ("produtos",)
    CAMPOS_OBRIGATORIOS = ("id", "title", "price", "category")

    def __init__(self, *, produtos, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context):
        if not self.produtos:
            raise ValueError("Nenhum produto recebido para validação")

        invalidos = []
        for produto in self.produtos:
            faltando = [
                campo
                for campo in self.CAMPOS_OBRIGATORIOS
                if produto.get(campo) in (None, "")
            ]
            if faltando:
                invalidos.append({"id": produto.get("id"), "campos_faltando": faltando})

        if invalidos:
            raise ValueError(f"Produtos com schema inválido: {invalidos}")

        self.log.info("%d produtos validados com sucesso", len(self.produtos))
        return self.produtos
