from diagrams import Diagram, Cluster
from diagrams.aws.compute import EKS
from diagrams.generic.database import SQL
from diagrams.onprem.compute import Server
from diagrams.generic.network import VPN

with Diagram("Network Diagram", show=False):

    with Cluster("AWS"):
        kube_cluster = EKS("Kube Cluster")
        airbyte_tool = EKS("Airbyte")

    with Cluster("On-Premise"):
        matrix_db = SQL("Matrix DB")
        alteryx_server = Server("Alteryx Server")

        with Cluster("Optional Route"):
            endpoint = VPN("Endpoint")
            matrix_db - endpoint
            endpoint >> kube_cluster

    matrix_db >> alteryx_server
    matrix_db >> kube_cluster >> airbyte_tool
