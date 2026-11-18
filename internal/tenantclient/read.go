// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"go.lodia.io/lodia/internal/jsonscan"
)

// MaxCauses bounds a Status's causes.
const MaxCauses = 64

var (
	ErrNotJSON = errors.New("the response is not JSON")

	// LongMessage is a Status's message past MaxMessageRaw.
	LongMessage = "the server's message is longer than " + strconv.Itoa(MaxMessageRaw) + " bytes"
)

// Do sends the request once, which asks for JSON, and returns the body of the
// answer: client-go sends a request again up to ten times on a 429 or a 5xx
// that carries Retry-After, and the next pass is the caller's retry. A body cut
// at MaxResponseBytes is ErrResponseTooLong whatever the status. A Status of
// Failure is the error of a failed request, as client-go's Result.Error reads
// it, but into the fields of a Status that the callers read. Any other body of
// a failed answer is the tenant's text, which client-go puts in its error, and
// which no log or message of a caller may carry: see noStatus. A request that
// fails before the head of its answer is read returns net/http's error, whose
// URL carries the query, and with it the continue token of a later page, up to
// 36,883 bytes: the error is the same with the path in the place of the URL.
func (c *Client) Do(ctx context.Context, request *rest.Request) ([]byte, error) {
	var cut bool
	body, err := request.MaxRetries(0).DoRaw(context.WithValue(ctx, cutKey{}, &cut))
	if cut {
		return nil, ErrResponseTooLong
	}
	if failed := new(url.Error); errors.As(err, &failed) {
		return nil, &url.Error{Op: failed.Op, URL: request.URL().Path, Err: failed.Err}
	}
	if apierrors.IsUnexpectedServerError(err) {
		if failure, ok := FailureOf(body); ok {
			return nil, &apierrors.StatusError{ErrStatus: failure}
		}
		return nil, noStatus(err)
	}
	return body, err
}

const NoStatusMessage = "the answer is no Status Health can read"

// noStatus is the error client-go built of a failed answer that is no Status
// within the bounds, without the tenant's text: its code, reason and
// Retry-After, and a message of our own, in the place of the body.
func noStatus(err error) error {
	var answered apierrors.APIStatus
	errors.As(err, &answered)
	status := answered.Status()
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    status.Code,
		Reason:  status.Reason,
		Message: NoStatusMessage,
		Details: &metav1.StatusDetails{
			Causes:            []metav1.StatusCause{{Type: metav1.CauseTypeUnexpectedServerResponse}},
			RetryAfterSeconds: status.Details.RetryAfterSeconds,
		},
	}}
}

type StatusBody struct {
	Status, Message string
	Reason          metav1.StatusReason
	Code            int32
	Details         struct {
		Causes Array[metav1.CauseType]
	}
}

func StatusFrom(data []byte) (s StatusBody, err error) {
	err = jsonscan.Walk("the Status", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "status":
			s.Status, err = jsonscan.Word("status", value, MaxWordRaw)
		case "message":
			s.Message, err = jsonscan.Text("message", value, MaxMessageRaw)
			if tooLong := new(jsonscan.TooLong); errors.As(err, &tooLong) {
				s.Message, err = LongMessage, nil
			}
		case "reason":
			var reason string
			reason, err = jsonscan.Text("reason", value, MaxReasonRaw)
			s.Reason = metav1.StatusReason(reason)
		case "code":
			var code *int32
			if code, err = jsonscan.Integer("code", value, MaxNumberRaw); code != nil {
				s.Code = *code
			}
		case "details":
			err = jsonscan.Walk("details", value, func(key, value []byte) (err error) {
				if string(key) == "causes" {
					s.Details.Causes, err = ArrayOf("details.causes", value, MaxCauses, CauseOf)
				}
				return err
			})
		}
		return err
	})
	return s, err
}

// CauseOf reads a cause's type, which a kube-apiserver writes under the key
// reason (metav1.StatusCause).
func CauseOf(data []byte) (c metav1.CauseType, err error) {
	err = jsonscan.Walk("a cause", data, func(key, value []byte) (err error) {
		if string(key) == "reason" {
			var reason string
			reason, err = jsonscan.Text("details.causes.reason", value, MaxReasonRaw)
			c = metav1.CauseType(reason)
		}
		return err
	})
	return c, err
}

func FailureOf(body []byte) (metav1.Status, bool) {
	if !json.Valid(body) {
		return metav1.Status{}, false
	}
	s, err := StatusFrom(body)
	if err != nil || s.Status != metav1.StatusFailure {
		return metav1.Status{}, false
	}
	failure := metav1.Status{Status: s.Status, Message: s.Message, Reason: s.Reason, Code: s.Code}
	for _, cause := range s.Details.Causes.Items {
		if failure.Details == nil {
			failure.Details = &metav1.StatusDetails{}
		}
		failure.Details.Causes = append(failure.Details.Causes, metav1.StatusCause{Type: cause})
	}
	return failure, true
}

// Decode reads a body, which encoding/json validates and nothing decodes, with
// the projection of its kind. A projection that fails returns what it read
// before the failure, with the error.
func Decode[T any](body []byte, err error, project func([]byte) (*T, error)) (*T, error) {
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, ErrNotJSON
	}
	out, err := project(body)
	if err != nil {
		return out, fmt.Errorf("read the response: %w", err)
	}
	return out, nil
}
