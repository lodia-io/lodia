// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// Authentication is the part of kube-apiserver's AuthenticationConfiguration
// a tenant sets; the anonymous section is Lodia's.
type Authentication struct {
	// JWT are JWT authenticators, kube-apiserver's apiserver.config.k8s.io/v1
	// JWTAuthenticator field for field.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.all(a, self.exists_one(b, b.issuer.url == a.issuer.url))",message="each issuer.url appears once"
	// +kubebuilder:validation:XValidation:rule="self.all(a, !has(a.issuer.discoveryURL) || size(a.issuer.discoveryURL) == 0 || self.exists_one(b, has(b.issuer.discoveryURL) && b.issuer.discoveryURL == a.issuer.discoveryURL))",message="each issuer.discoveryURL appears once"
	JWT []JWTAuthenticator `json:"jwt,omitempty"`
}

// JWTAuthenticator accepts the tokens of one issuer.
type JWTAuthenticator struct {
	// Issuer is where the tokens come from and whom they are for.
	// +required
	Issuer Issuer `json:"issuer"`

	// ClaimValidationRules are claims a token must hold, or CEL expressions on
	// its claims that must be true.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.all(r, !has(r.claim) || self.exists_one(s, has(s.claim) && s.claim == r.claim))",message="each claim appears once"
	// +kubebuilder:validation:XValidation:rule="self.all(r, !has(r.expression) || self.exists_one(s, has(s.expression) && s.expression == r.expression))",message="each expression appears once"
	ClaimValidationRules []ClaimValidationRule `json:"claimValidationRules,omitempty"`

	// ClaimMappings say which claims become the user.
	// +required
	ClaimMappings ClaimMappings `json:"claimMappings"`

	// UserValidationRules are CEL expressions on the user that must be true.
	// +optional
	// +listType=map
	// +listMapKey=expression
	// +kubebuilder:validation:MaxItems=8
	UserValidationRules []UserValidationRule `json:"userValidationRules,omitempty"`
}

// Issuer is where the tokens come from and whom they are for.
// +kubebuilder:validation:XValidation:rule="size(self.audiences) == 1 || has(self.audienceMatchPolicy) && self.audienceMatchPolicy == 'MatchAny'",message="audienceMatchPolicy must be MatchAny for multiple audiences"
// +kubebuilder:validation:XValidation:rule="!has(self.discoveryURL) || self.discoveryURL != self.url",message="discoveryURL must differ from url"
type Issuer struct {
	// URL is the issuer, the iss claim of its tokens.
	// +required
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.matches('^(?i:https)://[^/?#@]*([/?#].*)?$') && !self.matches('[?][^#]') && !self.matches('#.')",message="must be an https URL without a user, a query or a fragment"
	URL string `json:"url"`

	// DiscoveryURL is where the discovery document is fetched from, when not
	// under url.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="size(self) == 0 || self.matches('^(?i:https)://[^/?#@]*([/?#].*)?$') && !self.matches('[?][^#]') && !self.matches('#.')",message="must be an https URL without a user, a query or a fragment"
	DiscoveryURL *string `json:"discoveryURL,omitempty"`

	// CertificateAuthority is the PEM bundle the issuer's serving certificate
	// is verified against; the API server image's roots when empty.
	// +optional
	// +kubebuilder:validation:MaxLength=16384
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// Audiences are the aud values a token may carry.
	// +required
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=512
	Audiences []string `json:"audiences"`

	// AudienceMatchPolicy is MatchAny, required with more than one audience.
	// +optional
	// +kubebuilder:validation:Enum="";MatchAny
	AudienceMatchPolicy string `json:"audienceMatchPolicy,omitempty"`

	// EgressSelectorType is cluster: the API server reaches the issuer
	// through konnectivity, from the tenant's nodes.
	// +optional
	// +kubebuilder:default=cluster
	// +kubebuilder:validation:Enum="";cluster
	EgressSelectorType string `json:"egressSelectorType,omitempty"`
}

// ClaimValidationRule is a claim a token must hold, or a CEL expression on its
// claims that must be true.
// +kubebuilder:validation:XValidation:rule="has(self.claim) != has(self.expression)",message="set exactly one of claim and expression"
// +kubebuilder:validation:XValidation:rule="!has(self.claim) || !has(self.message) || size(self.message) == 0",message="message is set only with expression"
// +kubebuilder:validation:XValidation:rule="!has(self.expression) || !has(self.requiredValue) || size(self.requiredValue) == 0",message="requiredValue is set only with claim"
type ClaimValidationRule struct {
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	RequiredValue string `json:"requiredValue,omitempty"`
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Expression string `json:"expression,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Message string `json:"message,omitempty"`
}

// ClaimMappings say which claims become the user.
type ClaimMappings struct {
	// +required
	// +kubebuilder:validation:XValidation:rule="has(self.claim) != has(self.expression)",message="set exactly one of claim and expression"
	Username PrefixedClaimOrExpression `json:"username"`
	// +optional
	// +kubebuilder:validation:XValidation:rule="!(has(self.claim) && has(self.expression))",message="set at most one of claim and expression"
	Groups PrefixedClaimOrExpression `json:"groups,omitzero"`
	// +optional
	UID ClaimOrExpression `json:"uid,omitzero"`
	// +optional
	// +listType=map
	// +listMapKey=key
	// +kubebuilder:validation:MaxItems=8
	Extra []ExtraMapping `json:"extra,omitempty"`
}

// PrefixedClaimOrExpression is a claim with a prefix, or a CEL expression.
// +kubebuilder:validation:XValidation:rule="!has(self.claim) || has(self.prefix)",message="prefix is required with claim; it may be empty"
// +kubebuilder:validation:XValidation:rule="!has(self.expression) || !has(self.prefix)",message="prefix is set only with claim"
type PrefixedClaimOrExpression struct {
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Prefix *string `json:"prefix,omitempty"`
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Expression string `json:"expression,omitempty"`
}

// ClaimOrExpression is a claim or a CEL expression.
// +kubebuilder:validation:XValidation:rule="!(has(self.claim) && has(self.expression))",message="set at most one of claim and expression"
type ClaimOrExpression struct {
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim,omitempty"`
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Expression string `json:"expression,omitempty"`
}

// ExtraMapping is an extra attribute of the user, from a CEL expression.
type ExtraMapping struct {
	// +required
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.matches(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*/[-a-z0-9/._~%!$&\\x27()*+,;=:]+$')",message="must be a lowercase domain-prefixed path, such as example.org/foo"
	// +kubebuilder:validation:XValidation:rule="!self.split('/')[0].matches('(^|[.])(k8s|kubernetes)[.]io$')",message="k8s.io, kubernetes.io and their subdomains are reserved"
	Key string `json:"key"`
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	ValueExpression string `json:"valueExpression"`
}

// UserValidationRule is a CEL expression on the user that must be true.
type UserValidationRule struct {
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Expression string `json:"expression"`
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Message string `json:"message,omitempty"`
}
