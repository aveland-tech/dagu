// Copyright (C) 2024 The Daguflow/Dagu Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package dag

import (
	"github.com/daguflow/dagu/internal/frontend/gen/models"
	"github.com/go-openapi/swag"
)

type codedError struct {
	Code     int
	APIError *models.APIError
}

func newInternalError(err error) *codedError {
	return &codedError{Code: 500, APIError: &models.APIError{
		Message:         swag.String("Internal Server Error"),
		DetailedMessage: swag.String(err.Error()),
	}}
}

func newNotFoundError(err error) *codedError {
	return &codedError{Code: 404, APIError: &models.APIError{
		Message:         swag.String("Not Found"),
		DetailedMessage: swag.String(err.Error()),
	}}
}

func newBadRequestError(err error) *codedError {
	return &codedError{Code: 400, APIError: &models.APIError{
		Message:         swag.String("Bad Request"),
		DetailedMessage: swag.String(err.Error()),
	}}
}
