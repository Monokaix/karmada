/*
Copyright 2021 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"context"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"k8s.io/apimachinery/pkg/types"

	"github.com/karmada-io/karmada/pkg/generated/applyconfiguration/work/v1alpha2"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"github.com/karmada-io/karmada/pkg/util/names"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	applyconfigurationsmetav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/klog/v2"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
)

// GetBindingClusterNames will get clusterName list from bind clusters field
func GetBindingClusterNames(spec *workv1alpha2.ResourceBindingSpec) []string {
	var clusterNames []string
	for _, targetCluster := range spec.Clusters {
		clusterNames = append(clusterNames, targetCluster.Name)
	}
	return clusterNames
}

// IsBindingReplicasChanged will check if the sum of replicas is different from the replicas of object
func IsBindingReplicasChanged(bindingSpec *workv1alpha2.ResourceBindingSpec, strategy *policyv1alpha1.ReplicaSchedulingStrategy) bool {
	if strategy == nil {
		return false
	}
	if strategy.ReplicaSchedulingType == policyv1alpha1.ReplicaSchedulingTypeDuplicated {
		for _, targetCluster := range bindingSpec.Clusters {
			if targetCluster.Replicas != bindingSpec.Replicas {
				return true
			}
		}
		return false
	}
	if strategy.ReplicaSchedulingType == policyv1alpha1.ReplicaSchedulingTypeDivided {
		replicasSum := GetSumOfReplicas(bindingSpec.Clusters)
		return replicasSum != bindingSpec.Replicas
	}
	return false
}

// GetSumOfReplicas will get the sum of replicas in target clusters
func GetSumOfReplicas(clusters []workv1alpha2.TargetCluster) int32 {
	replicasSum := int32(0)
	for i := range clusters {
		replicasSum += clusters[i].Replicas
	}
	return replicasSum
}

// ConvertToClusterNames will convert a cluster slice to clusterName's sets.String
func ConvertToClusterNames(clusters []workv1alpha2.TargetCluster) sets.Set[string] {
	clusterNames := sets.New[string]()
	for _, cluster := range clusters {
		clusterNames.Insert(cluster.Name)
	}

	return clusterNames
}

// MergeTargetClusters will merge the replicas in two TargetCluster
func MergeTargetClusters(oldCluster, newCluster []workv1alpha2.TargetCluster) []workv1alpha2.TargetCluster {
	switch {
	case len(oldCluster) == 0:
		return newCluster
	case len(newCluster) == 0:
		return oldCluster
	}
	// oldMap is a map of the result for the old replicas so that it can be merged with the new result easily
	oldMap := make(map[string]int32)
	for _, cluster := range oldCluster {
		oldMap[cluster.Name] = cluster.Replicas
	}
	// merge the new replicas and the data of old replicas
	for i, cluster := range newCluster {
		value, ok := oldMap[cluster.Name]
		if ok {
			newCluster[i].Replicas = cluster.Replicas + value
			delete(oldMap, cluster.Name)
		}
	}
	for key, value := range oldMap {
		newCluster = append(newCluster, workv1alpha2.TargetCluster{Name: key, Replicas: value})
	}
	return newCluster
}

// RescheduleRequired judges whether reschedule is required.
func RescheduleRequired(rescheduleTriggeredAt, lastScheduledTime *metav1.Time) bool {
	if rescheduleTriggeredAt == nil {
		return false
	}
	// lastScheduledTime is nil means first schedule haven't finished or yet keep failing, just wait for this schedule.
	if lastScheduledTime == nil {
		return false
	}
	return rescheduleTriggeredAt.After(lastScheduledTime.Time)
}

// IsBindingSuspendScheduling return whether resource binding is scheduling suspended.
func IsBindingSuspendScheduling(rb *workv1alpha2.ResourceBinding) bool {
	if rb == nil || rb.Spec.Suspension == nil || rb.Spec.Suspension.Scheduling == nil {
		return false
	}
	return *rb.Spec.Suspension.Scheduling
}

// IsClusterBindingSuspendScheduling return whether cluster resource binding is scheduling suspended.
func IsClusterBindingSuspendScheduling(crb *workv1alpha2.ClusterResourceBinding) bool {
	if crb == nil || crb.Spec.Suspension == nil || crb.Spec.Suspension.Scheduling == nil {
		return false
	}
	return *crb.Spec.Suspension.Scheduling
}

// PatchBindingStatus patch resourceBinding status with SSA (Server Side Apply) to avoid patch status condition conflict with controllers.
func PatchBindingStatus(karmadaClient karmadaclientset.Interface, rb, updateRB *workv1alpha2.ResourceBinding, condition *metav1.Condition) error {
	if condition == nil {
		klog.Warningf("Skip patch ResourceBinding: %s as status condition is nil", klog.KObj(rb))
		return nil
	}

	applyCondition := applyconfigurationsmetav1.Condition().
		WithType(condition.Type).
		WithStatus(condition.Status).
		WithMessage(condition.Message).
		WithReason(condition.Reason).
		WithLastTransitionTime(condition.LastTransitionTime)
	applyStatus := v1alpha2.ResourceBindingStatus().WithConditions(applyCondition)

	applyStatus.WithSchedulerObservedGeneration(updateRB.Status.SchedulerObservedGeneration)
	applyRb := v1alpha2.ResourceBinding(rb.Name, rb.Namespace).WithStatus(applyStatus)

	_, err := karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).ApplyStatus(context.TODO(), applyRb, metav1.ApplyOptions{FieldManager: names.KarmadaSchedulerComponentName})
	if err != nil {
		klog.Errorf("Failed to patch schedule status ResourceBinding(%s/%s): %v", rb.Namespace, rb.Name, err)
		return err
	}

	klog.V(4).Infof("Patch schedule status to ResourceBinding(%s/%s) succeed", rb.Namespace, rb.Name)
	return nil
}

// PatchBindingStatusFiled patch specified status field of resource binding.
func PatchBindingStatusFiled(karmadaClient karmadaclientset.Interface, fieldName string, rb, updateRB *workv1alpha2.ResourceBinding) error {
	patchBytes, err := helper.GenFieldMergePatch(fieldName, rb.Status, updateRB.Status)
	if err != nil {
		return err
	}
	if len(patchBytes) == 0 {
		return nil
	}

	_, err = karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Patch(context.TODO(), rb.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if err != nil {
		klog.Errorf("Failed to patch schedule status ResourceBinding(%s/%s), filedName: %s, err: %v", rb.Namespace, rb.Name, fieldName, err)
		return err
	}
	klog.V(4).Infof("Patch schedule status to ResourceBinding(%s/%s) succeed, filedName: %s", rb.Namespace, rb.Name, fieldName)
	return nil
}
