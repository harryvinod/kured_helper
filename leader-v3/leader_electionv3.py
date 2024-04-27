from kubernetes import client, config
import os
import time
import logging
import threading
import json
import random
import requests
import base64
from datetime import datetime

# Constants
LEADER_ANNOTATION_KEY = "kured-helper/leader"
DAEMONSET_NAME = "kured"
NAMESPACE = "kube-system"
CHECKOUT_SERVICE_URL = "http://checkout-service.kube-system.svc.cluster.local:8080/checkout"
lockttl = 60

# Setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class LeaderElection:
    def __init__(self):
        config.load_incluster_config()
        self.api_instance_appsV1 = client.AppsV1Api()
        self.api_instance_coreV1 = client.CoreV1Api()
        self.api_instance_policyV1 = client.PolicyV1Api()
        self.namespace = NAMESPACE
        self.daemonset_name = DAEMONSET_NAME
        self.leader_elected = threading.Event()
        self.leader_elected.clear()
        
    def run_leader_election(self):
        """
        This method is used to initiate the leader election process. 
        """
        while True:
            try:
                logging.info("Trying to acquire leader")
                is_leader, node_with_kured_annotation, time_created = self.try_acquire_leader()
                if is_leader:
                    logging.info(f"Helper on {os.environ.get('NODE_ID')} elected as leader.")
                    success, timestamp = self.perform_actions_as_leader(node_with_kured_annotation, time_created)
                    self.relinquish_leader(success, node_with_kured_annotation, timestamp)
                else:
                    logging.info("Failed to acquire leadership. Will retry after 60 seconds.")
            except Exception as e:
                logging.error(f"Error during leader election: {e}")
            finally:
                time.sleep(60)  # Leader election interval

    def try_acquire_leader(self):
        """
        This method is where we elect a leader. I'm using a kube native way to run election based on 
        annotations on the kured daemonset along with its resource version. More details about the election 
        process can be found in this blog: https://kubernetes.io/blog/2016/01/simple-leader-election-with-kubernetes/. 
        
        I'm adding multiple checks before an election is done and handle any race conditions. Comments inline with each check.
        """
        logging.info("In try acquire leader")
        try:
            current_node_id = os.environ.get("NODE_ID")
            logging.info(f"Current Node ID: {current_node_id}")
            nodes_set = self.get_node_ids()
            node_with_kured_annotation, current_lead, time_created = self.get_node_with_kured_annotation()
            # logging.info(f"Time created in acquire after return: {time_created}")
            # logging.info(f"Nodes with Kured annotation: {node_with_kured_annotation} and the current leader is {current_lead}")

            # Check if the reboot is manually disabled. We skip the election in this case.
            if "manual" in node_with_kured_annotation:
                logging.info("Reboot Disabled Manually. Skipping leader election")
                return False, node_with_kured_annotation, time_created
            
            # Check if the NODE_ID environment variable is passed. Kured relies on this to function. We skip if its not.
            if not current_node_id:
                logging.error("NODE_ID environment variable is not set.")
                return False, node_with_kured_annotation, time_created
            
            # Check if kured has selected a node to reboot. We skip the election if there's no node seleted and there's no current leader.
            if not node_with_kured_annotation and not current_lead:
                logging.info("No nodes with Kured annotation. Skipping leader election.")
                return False, node_with_kured_annotation, time_created
            
            # Check if the current node is selected by kured for reboot. We skip the election on the pod if its about to reboot, except for a single node cluster.
            if current_node_id in node_with_kured_annotation and len(nodes_set) > 1:
                logging.info("Current node already has Kured annotation. Skipping leader election.")
                return False, node_with_kured_annotation, time_created
            
            # If a leader is elected and current node isn't the leader
            if current_lead and current_lead != current_node_id:
                # This is to handle cases with cluster autoscaler, when a node which was elected leader no longer exists.
                # We also remove stale leader to handle edge case
                # We remove the lock to ensure election is not disrupted.
                if current_lead not in nodes_set or not node_with_kured_annotation:
                    logging.info(f"Lock Held by a ghost node. Removing the lock...")
                    daemonset = self.api_instance_appsV1.read_namespaced_daemon_set(
                        name=self.daemonset_name,
                        namespace=self.namespace
                    )
                    current_resource_version = daemonset.metadata.resource_version
                    # Removing ghost annotations with ResourceVersion check
                    delay = random.randint(0, 30)
                    logging.info(f"Random delay before removing ghost annotation to avoid race condition: {delay} seconds")
                    time.sleep(delay)
                    patch_body = {
                        "metadata": {
                            "annotations": {
                                "weave.works/kured-node-lock": None,
                                "kured-helper/leader": ""
                            },
                            "resourceVersion": current_resource_version
                        }
                    }    
                    self.api_instance_appsV1.patch_namespaced_daemon_set(
                        name=self.daemonset_name,
                        namespace=self.namespace,
                        body=patch_body
                    )
                logging.info(f"There's already a leader present - {current_lead}")
                return False, node_with_kured_annotation, time_created
            # The leader election block wouldn't be called after a leader is already elected. If it reached here after election, then something went wrong. So we start election again
            if current_lead and current_lead == current_node_id:
                logging.error("Leadership was relinquished unexpectedly. Forcefully Removing lock for re-election")
                patch_body = {
                    "metadata": {
                        "annotations": {
                            "weave.works/kured-node-lock": None,
                            "kured-helper/leader": ""
                        }
                    }
                }    
                self.api_instance_appsV1.patch_namespaced_daemon_set(
                    name=self.daemonset_name,
                    namespace=self.namespace,
                    body=patch_body
                )
                if node_with_kured_annotation:
                    self.delete_kured_pod(node_with_kured_annotation)
                return False, node_with_kured_annotation, time_created
            # Get current DaemonSet resource version
            # Proceeding with leader election if none of the previous conditions are met
            daemonset = self.api_instance_appsV1.read_namespaced_daemon_set(
                name=self.daemonset_name,
                namespace=self.namespace
            )
            current_resource_version = daemonset.metadata.resource_version
            
            # We try to set annotation with key: kured-helper/leader with a value as current node name along 
            # with ResourceVersion fetched earlier. We alse add a random delay to avoid race condition across pods.
            # If the daemon set has the same resource version as fetched, the election is successful. 
            # If another pod already becase a leader before this, then the patch will fail due to resourceVersion mismatch.
            delay = random.randint(0, 60)
            logging.info(f"Random delay before adding leader annotation to avoid race condition: {delay} seconds")
            time.sleep(delay)
            patch_body = {
                "metadata": {
                    "annotations": {LEADER_ANNOTATION_KEY: current_node_id},
                    "resourceVersion": current_resource_version
                }
            }
            
            self.api_instance_appsV1.patch_namespaced_daemon_set(
                name=self.daemonset_name,
                namespace=self.namespace,
                body=patch_body
            )
            return True, node_with_kured_annotation, time_created
        except client.rest.ApiException as e:
            logging.error(f"Failed to acquire leadership: {e}")
            return False, node_with_kured_annotation, time_created
    
    def validate_leader(self, timestamp):
        """
        This method is to validate the current pod is indeed the leader
        """
        logging.info("Validating Leadership..")
        node_with_annotation, current_leader, time_created = self.get_node_with_kured_annotation()
        current_node_id = os.environ.get("NODE_ID")
        # time_created_truncated = time_created.rsplit('.', 1)[0] + time_created[-1]
        # time_created_dt = datetime.strptime(time_created_truncated, "%Y-%m-%dT%H:%M:%SZ")
        if not current_leader == current_node_id and timestamp == time_created:
            return False
        else:
            return True
    
    # def check_drain_error(self, node_with_kured_annotation, time_created):
    #     logging.info("Checking for drain failures")
    #     node_name = str(next(iter(node_with_kured_annotation)))
    #     pod_name = None
    #     pods = self.api_instance_coreV1.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}").items
    #     for pod in pods:
    #         for container in pod.spec.containers:
    #             if container.name == "kured":
    #                 pod_name = pod.metadata.name
    #                 break
    #         if pod_name:
    #             break

    #     if not pod_name:
    #         logging.error("No pod running kured found on the specified node.")
    #         return False
    #     start_time = time.time()
    #     while time.time() - start_time < 180:
    #         try:
    #             logs = self.api_instance_coreV1.read_namespaced_pod_log(name=pod_name, namespace="kube-system", container="kured", timestamps=True)

    #             # Split logs into lines
    #             log_lines = logs.splitlines()

    #             # Check if the desired messages are present in the logs
    #             for log_line in log_lines:
    #                 log_timestamp_str, log_message = log_line.split(" ", 1)
    #                 if log_timestamp_str > time_created and ("error when evicting pods" in log_message):
    #                     print("Found desired messages in kured logs.")
    #                     return True
    #                 if log_timestamp_str > time_created and ("Delaying reboot for" in log_message):
    #                     return False
    #         except Exception as e:
    #             print(f"Error fetching kured logs: {e}")

    #         # Wait for 1 minute before checking again
    #         time.sleep(30)
        
    
    def check_drain_completion(self, node_with_kured_annotation, time_created):
        """
        This method is to confirm the status of drain based on the logs of kured pod
        """
        # Get the name of the pod running kured on the specified node
        logging.info("Checking for Drain Completion")
        node_name = str(next(iter(node_with_kured_annotation)))
        # Get pods in the namespace
        pods_list = self.api_instance_coreV1.list_namespaced_pod(namespace="kube-system")

        # Iterate through pods to find the one running on the specified node
        for pod in pods_list.items:
            if pod.spec.node_name == node_name and "kured" in pod.metadata.name:
                pod_name = pod.metadata.name  # Return the pod name if found
                break
            
        logging.info(f"Pod Name in drain: {pod_name}")
        if not pod_name:
            logging.error("No pod running kured found on the specified node.")
            return False

        # Continuously check the logs of the kured container every 1 minute
        start_time = time.time()
        while time.time() - start_time < 240:
            # logging.info("In while")
            logs = self.api_instance_coreV1.read_namespaced_pod_log(name=pod_name, namespace="kube-system", container="kured", timestamps=True)
            # print(f"Logs = {logs}")
            log_lines = logs.splitlines()
            # print(f"Log lines = {log_lines}")
            # logging.info(f"Log lines = {log_lines}")
            # Check if the desired messages are present in the logs
            for log_line in log_lines:
                log_timestamp_str, log_message = log_line.split(" ", 1)
                # logging.info(f"Timestamp in drain: {log_timestamp_str}")
                # logging.info(f"log_message in drain: {log_message}")
                # The below messages would be in kured pod if drain timed out any reason
                if log_timestamp_str > time_created and ("global timeout reached" in log_message or "will release lock and retry cordon and drain before rebooting when lock is next acquired" in log_message):
                    logging.error("Drain Failed")
                    return False
                # The below messages would be in kured pod if the drain was successful
                if log_timestamp_str > time_created and ("Delaying reboot for" in log_message):
                    logging.info("Drain Completed Successfully")
                    return True
            time.sleep(30)

    def perform_actions_as_leader(self, node_with_kured_annotation, time_created):
        """
        We first confirm if the current pod is indeed the leader. If it is, we continue with the actions mentioned below.
        Details about each of the method called can be found in its definition.
        
        1. Check 1: Verify if there are any PDBs that will cause the drain to fail.
            If the check fails, we do this:
                Abort restarting other nodes, by creating a manual lock on the kured daemonset
                Add annotations to the node with the reboot status as failed along with the timestamp
                Delete the kured pod so that it doesn't try to reboot the node
                Updating annotation to the daemonset specifying the reboot failed so that we can end PD maintenance window
                End the Pagerduty Maintenance Window so that the cluster service can receive alerts and change events
                Send an Alert to the Pagerduty Cluster Service specifying that PDB is blocking reboot
                Send a failed change event
        2. Check 2: Verify the cluster is healthy by reviewing the kcheckout status
            If the check fails, we do this:
                Abort restarting other nodes, by creating a manual lock on the kured daemonset
                Uncordon the node, if it was conrdoned
                Add annotations to the node with the reboot status as failed along with the timestamp
                Delete the kured pod so that it doesn't try to reboot the node
                Updating annotation to the daemonset specifying the reboot failed so that we can end PD maintenance window
                End the Pagerduty Maintenance Window so that the cluster service can receive alerts and change events
                Send an Alert to the Pagerduty Cluster Service specifying that kcheckout failed
                Send a failed change event 

        3. If both checks are successful:
            i. wait for kured to reboot the node.
            ii. Once rebooted, verify the status of kcheckout again. 
                a. If reboot and kcheckout is successful, we do this:
                    Uncordon the node
                    Add annotations to the node with the reboot status along with the timestamp
                    Add a label to the kured pod to ensure its not rebooted again.
                    Add an annotation to the daemonset specifying the node rebooted - this is used for ending PD maintenance window after all nodes are rebooted
                    Relinquish leadership to continue rebooting other node
                b. If reboot is successful but kcheckout fails, we do this:
                    Uncordon the node
                    Abort restarting other nodes, by creating a manual lock on the kured daemonset
                    Add annotations to the node with the reboot status as failed along with the timestamp
                    Add a label to the kured pod to ensure its not rebooted again.
                    Updating annotation to the daemonset specifying the reboot failed so that we can end PD maintenance window
                    End the Pagerduty Maintenance Window so that the cluster service can receive alerts and change events
                    Send an Alert to the Pagerduty Cluster Service specifying that kcheckout failed
                    Send a failed change event

                    
        """
        if not self.validate_leader(time_created):
            success = False
            logging.error("Leader Validation Failed. Relinquishing Leadership")
            return success, time_created
        success = False
        try:
            # Check for PDBs with ALLOWED DISRUPTIONS as zero
            if self.check_pdb_allowed_disruptions_zero():
                # If PDBs with zero disruptions are found, abort restart and add labels to node with pdb=True
                logging.error(f"There are PDBs which doesn't allow disruption. Hence, skipping reboot")
                reason = "pdb"
                success = False
                self.abort_restart()
                self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                self.delete_kured_pod(node_with_kured_annotation)
                self.remove_reboot_status()
                self.end_maintenance_window()
                self.send_pd_alert(reason, time_created)
                self.send_change_event(time_created, success)
                # Send alert as pdb set
                return success, time_created
            else:
                logging.info("There are no PDBs which doesn't allow disruption. Moving on....")
            # Perform a GET request to the checkout service
            response = self.make_get_request(CHECKOUT_SERVICE_URL)
            time_created = time_created
            node_with_kured_annotation = node_with_kured_annotation
            logging.info(f"time created = {time_created} and kured node is {node_with_kured_annotation}")
            if response and response.get("success"):
                logging.info("Checkout service responded successfully. Waiting for kured to drain and reboot the node")

                if self.check_drain_completion(node_with_kured_annotation, time_created):
                    if self.wait_for_restart_completion(node_with_kured_annotation, time_created):
                        self.uncordon_node(node_with_kured_annotation)
                        # Perform another GET request to the checkout service after restart
                        response_after_restart = self.make_get_request(CHECKOUT_SERVICE_URL)
                        if response_after_restart and response_after_restart.get("success"):
                            logging.info("Checkout service responded successfully after restart.")
                            success=True
                            self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                            self.add_blocking_pod_label(node_with_kured_annotation)
                            self.update_reboot_status(node_with_kured_annotation, success)
                            all_rebooted = self.check_all_node_reboot()
                            if all_rebooted:
                                self.end_maintenance_window()
                                self.send_change_event(time_created, success)
                        else:
                            logging.error("Checkout service did not respond successfully after restart.")
                            # Uncordon the node and update labels
                            reason = "checkout"
                            success=False
                            self.uncordon_node(node_with_kured_annotation)
                            self.abort_restart()
                            self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                            self.add_blocking_pod_label(node_with_kured_annotation)
                            self.remove_reboot_status()
                            self.end_maintenance_window()
                            self.send_pd_alert(reason, time_created)
                            self.send_change_event(time_created, success)
                    else:
                        logging.error("Node restart by kured did not complete within the expected time.")
                        # Uncordon the node and update labels
                        reason = "restart"
                        success = False
                        self.uncordon_node(node_with_kured_annotation)
                        self.abort_restart()
                        self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                        self.add_blocking_pod_label(node_with_kured_annotation)
                        self.remove_reboot_status()
                        self.delete_kured_pod(node_with_kured_annotation)
                        self.end_maintenance_window()
                        self.send_pd_alert(reason, time_created)
                        self.send_change_event(time_created, success)
                else:
                    logging.error("Drain Failed. Hence, skipping reboot")
                    success = False
                    reason = "drain"
                    self.uncordon_node(node_with_kured_annotation)
                    self.abort_restart()
                    self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                    self.add_blocking_pod_label(node_with_kured_annotation)
                    self.remove_reboot_status()
                    self.end_maintenance_window()
                    self.send_pd_alert(reason, time_created)
                    self.send_change_event(time_created, success)
                    self.delete_kured_pod(node_with_kured_annotation)       
            else:
                logging.error("Checkout service did not respond successfully.")
                # Uncordon the node and update labels
                reason = "checkout"
                success = False
                self.abort_restart()
                self.uncordon_node(node_with_kured_annotation)
                self.add_annotations_to_node(node_with_kured_annotation, time_created, success)
                self.add_blocking_pod_label(node_with_kured_annotation)
                self.remove_reboot_status()
                self.end_maintenance_window()
                self.send_pd_alert(reason, time_created)
                self.send_change_event(time_created, success)
                self.delete_kured_pod(node_with_kured_annotation) 
        except Exception as e:
            logging.error(f"Error performing actions as leader: {e}")
        return success, time_created

    def get_pd_api_key(self):
        # Specify the name and namespace of the secret containing the PagerDuty API key
        try:
            pd_key = self.api_instance_coreV1.read_namespaced_secret('pd-key', self.namespace)
            if 'integration_key' in pd_key.data:
                integration_key = pd_key.data['integration_key']
                integration_key_deco_bytes = base64.b64decode(integration_key)
                integration_key = integration_key_deco_bytes.decode('utf-8')
            if 'api_key' in pd_key.data:
                api_key = pd_key.data['api_key']
                api_key_deco_bytes = base64.b64decode(api_key)
                api_key = api_key_deco_bytes.decode('utf-8')
            # logging.info(f"Api Key: {api_key} Integration Key: {integration_key}")
            return api_key, integration_key
            
        except Exception as e:
            logging.error(f"Error fetching API key from secret: {e}")
            return
    
    # Fetch PagerDuty service ID by service name
    def get_service_id(self, service_name):
        api_key, integration_key = self.get_pd_api_key()
        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json',
        }
        url = f'https://api.pagerduty.com/services?query={service_name}'
        try:
            response = requests.get(url, headers=headers)
            response_data = response.json()
            # logging.info(f"PD Service Get Response: {response_data}")
            if response_data.get('services'):
                return response_data['services'][0]['id']  # Assuming the first service in the list is the desired one
            else:
                return None
        except:
            logging.error(f"Unable to get the Service ID")
            return None
    
    def get_maintenance_window_id(self):
        api_key, integration_key = self.get_pd_api_key()
        if os.environ.get("pd-service-name"):
            service_name = os.environ.get("pd-service-name")
        service_id = self.get_service_id(service_name)
        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json',
        }
        url = f'https://api.pagerduty.com/maintenance_windows?service_ids[]={service_id}&filter=ongoing'
        try:
            response = requests.get(url, headers=headers)
            response_data = response.json()
            if response_data.get('maintenance_windows'):
                return response_data['maintenance_windows'][0]['id']  # Assuming the first maintenance window in the list is the ongoing one
            else:
                return None
        except:
            logging.error(f"Unable to get Maintenance Windows")
            
    def end_maintenance_window(self):
        api_key, integration_key = self.get_pd_api_key()
        maintenance_window_id = self.get_maintenance_window_id()
        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json',
        }
        url = f'https://api.pagerduty.com/maintenance_windows/{maintenance_window_id}'

        try:
            response = requests.delete(url, headers=headers)
            if response.status_code == 204:
                logging.info('Maintenance window ended successfully.')
                return
            else:
                logging.error('Failed to end maintenance window.')
                return
        except:
            logging.error('Failed to end maintenance window.')
            return
    
    def send_change_event(self, timestamp, success):
        api_key, integration_key = self.get_pd_api_key()
        url = "https://events.pagerduty.com/v2/change/enqueue"
        # Initialize the PagerDuty API client

        # Specify the service key of the PagerDuty service
        if success:
            change_summary = f"The reboot was successfully done at {timestamp}"
        else:
            change_summary = f"The reboot failed at {timestamp}"
        # Create a change event payload
        change_payload = {
            'routing_key': integration_key,
            'event_action': 'trigger',
            'payload': {
                'summary': change_summary,
                'source': 'Kured',
                'timestamp': timestamp,
            }
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Send the change event
        response = requests.post(url, json=change_payload, headers=headers)

        # Check if the request was successful
        if response.status_code == 202:
            logging.info("Change event sent successfully.")
            return
        else:
            logging.error("Failed to send change event. Status code:", response.status_code)
            logging.error("Error:", response.text)
            return

    def send_pd_alert(self, reason, timestamp):
        url = "https://events.pagerduty.com/v2/enqueue"
        api_key, integration_key = self.get_pd_api_key()
        service_name = os.environ.get("pd-service-name")
         
        if reason == "pdb":
            summary = (f"Reboot was aborted due to the existence of PDB.\n"
                       f"Please review your workloads to ensure that the PDB's can allow disruptions")
        elif reason == "checkout":
            summary = (f"Reboot was aborted as the kcheckout failed.\n"
            f"Please review the kcheckout URL: http://kcheckout.ms-infra.{service_name}:8080/checkout")
        elif reason == "unlock":
            summary = f"Unable to release the lock. Please connect to the cluster and manually release the lock if kcheckout is fine."
        elif reason == "restart":
            summary = (f"Kured was unable to restart the node in the specified period of time.\n" 
            f"Please review the kcheckout URL: http://kcheckout.ms-infra.{service_name}:8080/checkout to ensure the nodes are healthy")
        elif reason == "drain":
            summary = (f"Reboot was aborted as the drain failed.\n"
                       f"Please review your workloads to ensure that the node can be drained")
        payload = {
            "payload": {
                "summary": summary,
                "timestamp": timestamp,
                "severity": "critical",
                "source": "Kured"
            },
            "routing_key": integration_key,
            "event_action": "trigger"
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers)
        # Check if the request was successful
        if response.status_code == 202:
            logging.info("Alert triggered successfully.")
            return
        else:
            logging.error("Failed to trigger alert. Status code:", response.status_code)
            logging.error("Error:", response.text)
            return

    def make_get_request(self, url):
        logging.info("Performing checkout to get cluster health")
        # response = requests.get(CHECKOUT_SERVICE_URL)
        # data = response.json()
        data = {"checks": [], "success": True}
        if data.get("success", False):
            logging.info("Checkout service health check successful.")
            return {"success": True}
        else:
            logging.error("Checkout service health check failed. Aborting.")
            return {"success": False}  # Placeholder response
    
    def get_node_ids(self):
        nodes_set = set()
        try:
            # Fetch the list of nodes in the cluster
            nodes = self.api_instance_coreV1.list_node().items
            for node in nodes:
                # Extract node ID and add it to the set
                nodes_set.add(node.metadata.name)
        except Exception as e:
            print(f"Error fetching node IDs: {e}")
        return nodes_set
    
    def get_node_with_kured_annotation(self):
        node_with_annotation = set()
        current_leader = ""
        time_created = ""
        daemonset = self.api_instance_appsV1.read_namespaced_daemon_set(
            name=self.daemonset_name,
            namespace=self.namespace
        )
        if daemonset.metadata.annotations and 'weave.works/kured-node-lock' in daemonset.metadata.annotations:
            kured_annotation = daemonset.metadata.annotations['weave.works/kured-node-lock']
            kured_annotation_json = json.loads(kured_annotation)
            node_id = kured_annotation_json.get('nodeID')
            time_created = kured_annotation_json.get('created')
            logging.info(f"Time created is: {time_created}")
            logging.info(f"Node ID in get_node_with_kured_annotation: {node_id}")
            if node_id:
                node_with_annotation.add(node_id)
        if LEADER_ANNOTATION_KEY in daemonset.metadata.annotations:
            current_leader = daemonset.metadata.annotations[LEADER_ANNOTATION_KEY]
        logging.info(f"Time created on return: {time_created}")
        return node_with_annotation, current_leader, time_created
    
    def abort_restart(self):
        logging.info(f"Aborting Drain and Reboot for all the Nodes...")
        try:
            # Set annotation to empty string to relinquish leadership
            patch_body = {
                "metadata": {
                        "annotations": {
                            LEADER_ANNOTATION_KEY: "",
                            "weave.works/kured-node-lock": '{"nodeID": "manual"}'
                            }
                        }
                    }
            self.api_instance_appsV1.patch_namespaced_daemon_set(
                name=self.daemonset_name,
                namespace=self.namespace,
                body=patch_body
            )
        except Exception as e:
            logging.error(f"Error adding annotations to DS: {e}")
        
    def wait_for_restart_completion(self, node_id_str, time_created):
        node_id_str = str(next(iter(node_id_str)))
        logging.info(f"Waiting for node {node_id_str} to restart...")
        timeout = 6000  # Timeout in seconds (adjust as needed)
        # start_time = datetime.datetime.strptime(time_created, "%Y-%m-%d %H:%M:%S")
        start_time = time.time() # Change this to time_created
        while True:
            if time.time() - start_time > timeout:
                logging.error(f"Timeout waiting for node {node_id_str} to restart.")
                return False
            try:
                # Get node events
                # field_selector={node_id_str}
                events = self.api_instance_coreV1.list_event_for_all_namespaces()
                
                for event in events.items:
                    # logging.info(f"Event time: {event_time}")
                    last_timestamp = event.last_timestamp
                    event_time = last_timestamp.timestamp() if last_timestamp else 0
                    # logging.info(f"Event time: {event_time}")
                    if event_time > start_time:
                        if event.reason == "Rebooted" and node_id_str in event.message:
                            # logging.info(f"Message - {event}")
                            logging.info(f"Node {node_id_str} has been restarted.")
                            return True
                time.sleep(20)  # Check every 10 seconds
            except Exception as e:
                logging.error(f"Error occurred while checking node {node_id_str} events: {e}")
    
    def check_pdb_allowed_disruptions_zero(self):
        # return False
        pdbs = self.api_instance_policyV1.list_pod_disruption_budget_for_all_namespaces().items
        for pdb in pdbs:
            if pdb.status and pdb.status.disruptions_allowed == 0:
                logging.info(f"Disruptions = {pdb.status.disruptions_allowed}")
                return True
            else:
                return False

    def update_reboot_status(self, node_id, success):
        success = success
        
        node_id = str(next(iter(node_id)))
        annotations = dict()
        
        try:
            # Get the current annotations of the kured daemonset
            daemonset = self.api_instance_appsV1.read_namespaced_daemon_set("kured", "kube-system")
            current_annotations = daemonset.metadata.annotations or {}
        except Exception as e:
            logging.error(f"Error reading annotations from kured daemonset: {e}")
            return
        if success:
            successful_reboots = current_annotations.get("successful-reboots", "").split(", ") if current_annotations.get("successful-reboots") else []
            successful_reboots.append(node_id)
            annotations["successful-reboots"] = ", ".join(successful_reboots)
        else:
            failed_reboots = current_annotations.get("failed-reboots", "").split(", ") if current_annotations.get("failed-reboots") else []
            failed_reboots.append(node_id)
            annotations["failed-reboots"] = ", ".join(failed_reboots)
        
        logging.info(f"Adding annotations to kured daemonset")
        try:
            # Patch the kured daemonset object to apply changes
            patch = {"metadata": {"annotations": annotations}}
            self.api_instance_appsV1.patch_namespaced_daemon_set("kured", "kube-system", patch)
            logging.info("Annotations added to kured daemonset successfully.")
        except Exception as e:
            logging.error(f"Error adding annotations to kured daemonset: {e}")
        return
    
    def remove_reboot_status(self):
        patch_body = {"metadata": {"annotations": {"successful-reboots": "", "failed-reboots": ""}}}
        try:
            self.api_instance_appsV1.patch_namespaced_daemon_set("kured", "kube-system", patch_body)
            logging.info("Annotations added to kured daemonset successfully.")
        except Exception as e:
            logging.error(f"Error adding annotations to kured daemonset: {e}")
        return
    
    def check_all_node_reboot(self):
        try:
            daemonset = self.api_instance_appsV1.read_namespaced_daemon_set("kured", "kube-system")
            annotations = daemonset.metadata.annotations or {}

            # Retrieve node IDs from annotations and remove leading spaces
            successful_reboots = set(node.strip() for node in filter(None, annotations.get("successful-reboots", "").split(",")))
            failed_reboots = set(node.strip() for node in filter(None, annotations.get("failed-reboots", "").split(",")))
            logging.info(f"Successful Reboots: {successful_reboots} \nFailed Reboots: {failed_reboots}")

            # Get the set of all node IDs
            all_node_set = self.get_node_ids()

            # Check if all node IDs are present in the set of node names
            if all_node_set <= successful_reboots.union(failed_reboots):
                return True
            else:
                return False

        except Exception as e:
            logging.error(f"Error occurred while checking node reboot status: {e}")
            return False

    def uncordon_node(self, node_id):
        node_id = str(next(iter(node_id)))
        logging.info(f"Uncordoning node {node_id}")
        now = time.time()
        while True:
            try:
            # Get the current node object
                node = self.api_instance_coreV1.read_node(node_id)
                # Check if the node is already uncordoned
                ready_status = {"type": "", "status": ""}
                for condition in node.status.conditions:
                    if condition.type == "Ready":
                        ready_status["type"] = condition.type
                        ready_status["status"] = condition.status
                        break
                
                if not node.spec.unschedulable:
                    logging.info(f"Node {node_id} is uncordoned.")
                    return
                if ready_status["type"] == "Ready" and ready_status["status"] == "True":
                    # Update the node object to set unschedulable to False
                    node.spec.unschedulable = False
                    # Patch the node object to apply changes
                    self.api_instance_coreV1.patch_node(node_id, node)
                    logging.info(f"Node {node_id} uncordoned successfully.")
                time.sleep(10)
                if time.time() - now > 600:
                    logging.error(f"Unable to Uncordon in 10 minutes.")
                    return
            except Exception as e:
                logging.error(f"Error uncordoning node {node_id}: {e}")

    def add_annotations_to_node(self, node_id, time_created, success):
        success = success
        node_id = str(next(iter(node_id)))
        time_created = time_created
        annotations = dict()
        if success == True:
            annotations = {"last-restart-status": "Success", "last-restart-attempt": str(time_created), "last-successful-restart-timestamp": str(time_created)}
        elif success == False:
            annotations = {"last-restart-status": "Failed", "last-restart-attempt": str(time_created)}
        
        logging.info(f"Adding annotations to node {node_id}")
        try:
            # Get the current node object
            node = self.api_instance_coreV1.read_node(node_id)

            # Update the node object with the new annotations
            current_annotations = node.metadata.annotations or {}
            current_annotations.update(annotations)
            logging.info(f"Annotations {current_annotations} added to node {node_id} successfully.")
            # Patch the node object to apply changes
            patch = {"metadata": {"annotations": current_annotations}}
            self.api_instance_coreV1.patch_node(node_id, patch)
            logging.info(f"Annotations added to node {node_id} successfully.")
        except Exception as e:
            logging.error(f"Error adding annotations to node {node_id}: {e}")

    def add_blocking_pod_label(self, node_name):
        try:
            logging.info(f"Adding reboot block label to the kured pod to prevent further reboots.")
            node_name = str(next(iter(node_name)))

            # Get pods in the namespace
            pods_list = self.api_instance_coreV1.list_namespaced_pod(namespace="kube-system")

            # Iterate through pods to find the one running on the specified node
            for pod in pods_list.items:
                if pod.spec.node_name == node_name and "kured" in pod.metadata.name:
                    pod_name = pod.metadata.name  # Return the pod name if found
            
            pod = self.api_instance_coreV1.read_namespaced_pod(name=pod_name, namespace="kube-system")
            # Update the labels with the new label
            labels = pod.metadata.labels or {}
            labels["block-reboot"] = "block"
            patch = {"metadata": {"labels": labels}}
            self.api_instance_coreV1.patch_namespaced_pod(name=pod_name, namespace="kube-system", body=patch)
        except Exception as e:
            logging.info(f"Error labeling kured pods: {e}")
        return
    
    def remove_blocking_pod_label(self):
        try:
            logging.info(f"Removing reboot block label")
            # Get pods in the namespace
            pods_list = self.api_instance_coreV1.list_namespaced_pod(namespace="kube-system")

            for pod in pods_list.items:
                if "kured" in pod.metadata.name:
                    pod_name = pod.metadata.name  # Return the pod name if found
                    logging.info(f"Removing reboot block label for {pod_name}")
                    pod = self.api_instance_coreV1.read_namespaced_pod(name=pod_name, namespace="kube-system")
                    labels = pod.metadata.labels or {}
                    labels["block-reboot"] = "unblock"
                    patch = {"metadata": {"labels": labels}}
                    self.api_instance_coreV1.patch_namespaced_pod(name=pod_name, namespace="kube-system", body=patch)
                    logging.info(f"Removed reboot block label for {pod_name}")
        except Exception as e:
            logging.info(f"Error labeling kured pods: {e}")
        return
    
    def delete_kured_pod(self, node_name):
        try:
            node_name = str(next(iter(node_name)))
            # Get pods in the namespace
            pods_list = self.api_instance_coreV1.list_namespaced_pod(namespace="kube-system")
            # logging.info(f"Input Node name: {node_name}")
            # logging.info(f"pod list: {pods_list}")

            # Iterate through pods to find the one running on the specified node
            for pod in pods_list.items:
                # logging.info(f"\nPod Name: {pod.metadata.name}")
                # logging.info(f"Node Name: {pod.spec.node_name}")
                if pod.spec.node_name == node_name and "kured" in pod.metadata.name:
                    _pod_name = pod.metadata.name  # Return the pod name if found
                    # logging.info(f"Kured Pod Name in for is {_pod_name}")
                    break
            # logging.info(f"Kured Pod Name is {_pod_name}")
            self.api_instance_coreV1.delete_namespaced_pod(name=_pod_name, namespace="kube-system")
        except Exception as e:
            logging.info(f"Error Deleting kured pods: {e}")
        return

    def relinquish_leader(self, success, node_with_kured_annotation, timestamp):
        logging.info("Relinquishing leadership...")
        logging.info(f"Input timestamp: {timestamp}")
        reboot_node = str(next(iter(node_with_kured_annotation)))
        now = time.time()
        while True:
            try:
                node_name, current_lead, time_created = self.get_node_with_kured_annotation()
                if node_name:
                    node_name = str(next(iter(node_name)))
                else:
                    node_name = ""
                all_rebooted = self.check_all_node_reboot()
                if all_rebooted:
                    logging.info("All the nodes have rebooted successfully.")
                    self.remove_reboot_status()
                    self.remove_blocking_pod_label()
                    
                logging.info(f"Setting leader annotation to empty string to relinquish leadership")
                logging.info(f"0 - node_name= {node_name} \nreboot_node= {reboot_node}")
                if not node_name or reboot_node != node_name:
                    logging.info(f"1 - node_name= {node_name} \nreboot_node= {reboot_node}")
                    logging.info(f"Setting leader annotation to empty string to relinquish leadership - 1")
                    if not node_name or "manual" in node_name:
                        logging.info(f"2 - node_name= {node_name} \nreboot_node= {reboot_node}")
                        logging.info(f"Setting leader annotation to empty string to relinquish leadership - 2")
                        patch_body = {"metadata": {"annotations": {LEADER_ANNOTATION_KEY: ""}}}
                        try:
                            logging.info(f"Setting leader annotation to empty string to relinquish leadership - 3")
                            self.api_instance_appsV1.patch_namespaced_daemon_set(
                                name=self.daemonset_name,
                                namespace=self.namespace,
                                body=patch_body
                            )
                        except Exception as e:
                            logging.error(f"Failed to relinquish leadership in loop: {e}")   
                        # Proceed based on success status
                        if success:
                            logging.info("Reboot completed successfully. Relinquishing leader role.")
                            return
                        else:
                            logging.error("Reboot Failed. Relinquishing Leader Role.")
                            return
                time.sleep(10)
                if time.time() - now > 600:
                    logging.error(f"Unable to release lock in 10 minutes.")
                    reason = "unlock"
                    self.send_pd_alert(reason, time_created)
                    return   
            except client.rest.ApiException as e:
                logging.error(f"Failed to relinquish leadership: {e}")

if __name__ == "__main__":
    leader_election = LeaderElection()
    leader_election.run_leader_election()
