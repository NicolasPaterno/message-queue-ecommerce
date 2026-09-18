implementar um sistema de carrinho. ao começar o processo de pagamento o sistema faz um pre-order temporário para o cliente que começou o pagamento primeiro não fique sem estoque caso outros clientes peçam antes e finalizem de maneira concorrente, assim o cliente só pode começar o processo de pagamento do porduto se o produto possuir estouqe e não estiver em preorder



carrinho -> order -> pré-aloca item -1 estoque de maneira temporária até dar timeout no tempo mínimo de compra ou até finalizar/cancelar o order/compra.